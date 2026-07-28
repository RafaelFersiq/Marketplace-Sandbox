"""Avro serialization, deserialization, and schema validation module.

Provides helper classes and functions for validating Python payloads against Avro schemas,
encoding payloads to Avro binary format (with support for Confluent Schema Registry
5-byte wire format headers), and decoding binary payloads back to dictionaries.
"""

import json
import logging
import struct
from io import BytesIO
from typing import Any

import fastavro
from fastavro.validation import ValidationError

from ingestion.schema_registry import SchemaRegistryManager

logger = logging.getLogger("serialization")

WIRE_FORMAT_MAGIC_BYTE = b"\x00"
WIRE_FORMAT_HEADER_SIZE = 5


class SerializationError(Exception):
    """Raised when Avro payload serialization fails."""


class DeserializationError(Exception):
    """Raised when Avro payload deserialization fails."""


class SchemaValidationError(SerializationError):
    """Raised when payload validation against Avro schema fails."""


class AvroSerializerDeserializer:
    """Helper for Avro payload validation, binary serialization, and deserialization."""

    def __init__(self, schema_registry_manager: SchemaRegistryManager | None = None) -> None:
        """Initialize the AvroSerializerDeserializer instance.

        Args:
            schema_registry_manager: Optional SchemaRegistryManager instance used for retrieving
                schemas by ID during deserialization.
        """
        self.schema_registry_manager = schema_registry_manager
        self._parsed_schemas_cache: dict[str, dict[str, Any]] = {}

    def _get_parsed_schema(self, schema_dict: dict[str, Any]) -> dict[str, Any]:
        """Parse and cache an Avro schema dictionary.

        Args:
            schema_dict: Raw Avro schema dictionary.

        Returns:
            Parsed fastavro schema object.
        """
        schema_key = json.dumps(schema_dict, sort_keys=True)
        if schema_key not in self._parsed_schemas_cache:
            self._parsed_schemas_cache[schema_key] = fastavro.parse_schema(schema_dict)
        return self._parsed_schemas_cache[schema_key]

    def validate(self, payload: dict[str, Any], schema: dict[str, Any]) -> bool:
        """Validate a Python dictionary payload against an Avro schema.

        Args:
            payload: Event data dictionary.
            schema: Avro schema dictionary.

        Returns:
            True if validation succeeds.

        Raises:
            SchemaValidationError: If payload violates the schema contract.
        """
        parsed_schema = self._get_parsed_schema(schema)
        schema_name = schema.get("name", "UnknownRecord")

        try:
            is_valid = fastavro.validate(payload, parsed_schema, raise_errors=True)
            if not is_valid:
                raise SchemaValidationError(f"Payload failed validation against schema '{schema_name}'.")
            return True
        except (ValidationError, TypeError, ValueError, KeyError) as exc:
            logger.warning(
                "Payload validation failed for schema '%s': %s",
                schema_name,
                exc,
                extra={"schema_name": schema_name},
            )
            raise SchemaValidationError(f"Payload validation failed for schema '{schema_name}': {exc}") from exc

    def serialize(
        self,
        payload: dict[str, Any],
        schema: dict[str, Any],
        schema_id: int | None = None,
        include_wire_header: bool = True,
    ) -> bytes:
        """Serialize a Python dictionary payload to binary Avro format.

        Args:
            payload: Event data dictionary.
            schema: Avro schema dictionary.
            schema_id: Confluent Schema Registry schema ID (required if include_wire_header is True).
            include_wire_header: If True, prepends 5-byte Confluent wire format header
                (magic byte 0x00 + 4-byte big-endian schema_id).

        Returns:
            Encoded binary bytes.

        Raises:
            SchemaValidationError: If payload fails validation against schema.
            SerializationError: If serialization or wire header formatting fails.
        """
        self.validate(payload, schema)
        parsed_schema = self._get_parsed_schema(schema)

        buffer = BytesIO()
        if include_wire_header:
            if schema_id is None:
                raise SerializationError("schema_id must be provided when include_wire_header is True.")
            header = WIRE_FORMAT_MAGIC_BYTE + struct.pack(">I", schema_id)
            buffer.write(header)

        try:
            fastavro.schemaless_writer(buffer, parsed_schema, payload)
            return buffer.getvalue()
        except Exception as exc:
            logger.error("Avro binary serialization failed: %s", exc)
            raise SerializationError(f"Avro binary serialization failed: {exc}") from exc

    def deserialize(
        self,
        data: bytes,
        reader_schema: dict[str, Any] | None = None,
        has_wire_header: bool = True,
    ) -> tuple[int | None, dict[str, Any]]:
        """Deserialize binary Avro data back into a Python dictionary.

        Args:
            data: Binary Avro payload.
            reader_schema: Optional Avro schema dict for decoding.
            has_wire_header: If True, expects 5-byte Confluent wire format header.

        Returns:
            Tuple of (schema_id, decoded_payload_dict). schema_id will be None if has_wire_header is False.

        Raises:
            DeserializationError: If byte header is invalid, schema cannot be retrieved, or Avro decoding fails.
        """
        if not data:
            raise DeserializationError("Cannot deserialize empty byte payload.")

        buffer = BytesIO(data)
        schema_id: int | None = None

        if has_wire_header:
            if len(data) < WIRE_FORMAT_HEADER_SIZE:
                raise DeserializationError(
                    f"Invalid wire format message: length ({len(data)}) is shorter "
                    f"than header size ({WIRE_FORMAT_HEADER_SIZE})."
                )
            magic_byte = buffer.read(1)
            if magic_byte != WIRE_FORMAT_MAGIC_BYTE:
                raise DeserializationError(f"Invalid magic byte in wire format: expected 0x00, got {magic_byte!r}.")
            schema_id = struct.unpack(">I", buffer.read(4))[0]

            if reader_schema is None:
                if self.schema_registry_manager is not None:
                    reader_schema = self.schema_registry_manager.get_schema_by_id(schema_id)
                else:
                    raise DeserializationError(
                        f"No reader schema provided and SchemaRegistryManager unavailable "
                        f"to resolve schema ID {schema_id}."
                    )
        else:
            if reader_schema is None:
                raise DeserializationError(
                    "reader_schema is required when decoding raw Avro binary without wire header."
                )

        parsed_schema = self._get_parsed_schema(reader_schema)

        try:
            decoded_payload = fastavro.schemaless_reader(buffer, parsed_schema)
            return schema_id, decoded_payload
        except Exception as exc:
            logger.error("Avro binary deserialization failed: %s", exc)
            raise DeserializationError(f"Avro binary deserialization failed: {exc}") from exc
