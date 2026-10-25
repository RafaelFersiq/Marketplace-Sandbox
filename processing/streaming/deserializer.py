"""Confluent Avro wire-format deserializer and PySpark Structured Streaming integration.

Extracts the 5-byte Confluent wire format header (0x00 magic byte + 4-byte big-endian schema ID),
resolves schemas from Confluent Schema Registry with multi-level in-memory caching, decodes binary
Avro payloads using fastavro, validates business envelope contracts (event_id, event_type, timestamp),
and isolates corrupted or malformed payloads without halting PySpark streaming queries.
"""

import base64
import json
import logging
import os
import struct
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any

import fastavro
from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from ingestion.producers.config import DEFAULT_TOPIC_SCHEMA_STEM_MAP
from ingestion.schema_registry import (
    SchemaNotFoundError,
    SchemaRegistryError,
    SchemaRegistryManager,
)
from ingestion.serialization import (
    WIRE_FORMAT_HEADER_SIZE,
    WIRE_FORMAT_MAGIC_BYTE,
)

logger = logging.getLogger("processing.streaming.deserializer")


class WireFormatError(ValueError):
    """Raised when wire format header validation or extraction fails."""

    def __init__(self, message: str, error_type: str) -> None:
        """Initialize WireFormatError with descriptive message and standardized error type."""
        super().__init__(message)
        self.error_type = error_type


@dataclass(frozen=True)
class DeserializedRecord:
    """Immutable data container representing the result of a deserialization attempt."""

    is_valid: bool
    schema_id: int | None
    event_id: str | None
    event_type: str | None
    event_timestamp_ms: int | None
    payload_json: str | None
    error_type: str | None
    error_message: str | None
    raw_payload_base64: str | None

    def to_dict(self) -> dict[str, Any]:
        """Convert record fields into a serializable dictionary.

        Returns:
            Dictionary mapping field names to record values.
        """
        return {
            "is_valid": self.is_valid,
            "schema_id": self.schema_id,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_timestamp_ms": self.event_timestamp_ms,
            "payload_json": self.payload_json,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "raw_payload_base64": self.raw_payload_base64,
        }


def _json_default_serializer(val: Any) -> Any:
    """JSON serialization fallback for non-primitive types like Decimal, datetime, and bytes."""
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, bytes):
        return base64.b64encode(val).decode("ascii")
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val)


class ConfluentWireDeserializer:
    """Confluent wire format Avro deserializer with schema caching and fault isolation."""

    def __init__(
        self,
        schema_registry_url: str | None = None,
        schema_registry_manager: SchemaRegistryManager | None = None,
        schemas_dir: Path | str | None = None,
    ) -> None:
        """Initialize ConfluentWireDeserializer.

        Args:
            schema_registry_url: URL for Confluent Schema Registry (e.g. http://localhost:8081).
            schema_registry_manager: Optional existing SchemaRegistryManager client instance.
            schemas_dir: Optional filesystem path to local Avro schema directory for preloading.
        """
        self.schema_registry_url = schema_registry_url or os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        self.schema_registry_manager = schema_registry_manager or SchemaRegistryManager(url=self.schema_registry_url)
        self.schemas_dir = Path(schemas_dir) if schemas_dir else None
        self._schema_cache: dict[int, dict[str, Any]] = {}
        self._parsed_fastavro_cache: dict[int, Any] = {}

        if self.schemas_dir and self.schemas_dir.is_dir():
            self.preload_schemas_from_dir(self.schemas_dir)

    def preload_schemas_from_dir(self, schemas_dir: Path | str) -> None:
        """Load and inspect schema files from a local directory.

        Args:
            schemas_dir: Directory containing .avsc schema definitions.
        """
        path = Path(schemas_dir)
        if not path.is_dir():
            logger.warning("Schemas directory '%s' does not exist.", path)
            return

        for schema_file in path.glob("*.avsc"):
            try:
                SchemaRegistryManager.load_schema_from_file(schema_file)
                logger.debug(
                    "Inspected schema file '%s' corresponding to registered topics: %s",
                    schema_file.name,
                    list(DEFAULT_TOPIC_SCHEMA_STEM_MAP.keys()),
                )
            except Exception as exc:
                logger.warning("Failed to inspect schema file %s: %s", schema_file, exc)

    def register_cached_schema(self, schema_id: int, schema_dict: dict[str, Any]) -> None:
        """Register a pre-resolved schema dictionary into local fastavro parsed cache.

        Args:
            schema_id: Confluent schema registry identifier integer.
            schema_dict: Avro schema dictionary.
        """
        try:
            parsed_schema = fastavro.parse_schema(schema_dict)
            self._schema_cache[schema_id] = schema_dict
            self._parsed_fastavro_cache[schema_id] = parsed_schema
            if self.schema_registry_manager is not None:
                cache = getattr(self.schema_registry_manager, "_schema_id_cache", None)
                if isinstance(cache, dict):
                    cache[schema_id] = schema_dict
            logger.debug("Cached parsed Avro schema for ID %d.", schema_id)
        except Exception as exc:
            logger.error("Failed to parse and cache schema ID %d: %s", schema_id, exc)
            raise

    def get_parsed_schema(self, schema_id: int) -> Any:
        """Retrieve parsed fastavro schema by ID, utilizing in-memory cache or remote registry.

        Args:
            schema_id: Confluent schema registry identifier integer.

        Returns:
            Parsed fastavro schema structure.

        Raises:
            SchemaNotFoundError: If schema ID is not present in registry.
            SchemaRegistryError: If remote request fails.
        """
        if schema_id in self._parsed_fastavro_cache:
            return self._parsed_fastavro_cache[schema_id]

        if self.schema_registry_manager is None:
            raise SchemaRegistryError(f"No SchemaRegistryManager available to resolve schema ID {schema_id}.")

        schema_dict = self.schema_registry_manager.get_schema_by_id(schema_id)
        parsed_schema = fastavro.parse_schema(schema_dict)
        self._schema_cache[schema_id] = schema_dict
        self._parsed_fastavro_cache[schema_id] = parsed_schema
        return parsed_schema

    @staticmethod
    def parse_wire_header(raw_bytes: bytes | None) -> tuple[int, bytes]:
        """Validate Confluent 5-byte wire format header and extract schema ID and payload.

        Args:
            raw_bytes: Raw binary message from Kafka.

        Returns:
            Tuple of (schema_id, avro_binary_payload).

        Raises:
            WireFormatError: If payload is empty, truncated, or lacks 0x00 magic byte.
        """
        if not raw_bytes:
            raise WireFormatError("Payload is empty or None.", "EMPTY_PAYLOAD")

        if len(raw_bytes) < WIRE_FORMAT_HEADER_SIZE:
            raise WireFormatError(
                f"Payload length ({len(raw_bytes)}) is shorter than "
                f"Confluent header ({WIRE_FORMAT_HEADER_SIZE} bytes).",
                "TRUNCATED_HEADER",
            )

        magic_byte = raw_bytes[:1]
        if magic_byte != WIRE_FORMAT_MAGIC_BYTE:
            raise WireFormatError(
                f"Invalid wire format magic byte: expected 0x00, got {magic_byte!r}.",
                "INVALID_MAGIC_BYTE",
            )

        schema_id = struct.unpack(">I", raw_bytes[1:5])[0]
        avro_payload = raw_bytes[5:]
        return schema_id, avro_payload

    def deserialize(self, raw_bytes: bytes | None, topic: str | None = None) -> DeserializedRecord:
        """Safely deserialize binary payload adhering to Confluent Avro wire specification.

        Guarantees no unhandled exceptions are raised on corrupt, malformed, or invalid payloads.

        Args:
            raw_bytes: Raw bytes received from Kafka consumer.
            topic: Optional Kafka topic name for contextual diagnostics.

        Returns:
            DeserializedRecord populated with parsed event data or diagnostic error fields.
        """
        raw_b64: str | None = None
        if raw_bytes is not None and len(raw_bytes) > 0:
            raw_b64 = base64.b64encode(raw_bytes).decode("ascii")

        # Step 1: Parse Confluent 5-byte wire header
        try:
            schema_id, avro_payload = self.parse_wire_header(raw_bytes)
        except WireFormatError as exc:
            logger.warning(
                "Wire format validation error on topic '%s': %s",
                topic,
                exc,
                extra={"error_type": exc.error_type, "topic": topic},
            )
            return DeserializedRecord(
                is_valid=False,
                schema_id=None,
                event_id=None,
                event_type=None,
                event_timestamp_ms=None,
                payload_json=None,
                error_type=exc.error_type,
                error_message=str(exc),
                raw_payload_base64=raw_b64 or "",
            )
        except Exception as exc:
            logger.error("Unexpected wire header extraction error on topic '%s': %s", topic, exc)
            return DeserializedRecord(
                is_valid=False,
                schema_id=None,
                event_id=None,
                event_type=None,
                event_timestamp_ms=None,
                payload_json=None,
                error_type="WIRE_FORMAT_ERROR",
                error_message=str(exc),
                raw_payload_base64=raw_b64 or "",
            )

        # Step 2: Resolve schema definition from cache or Schema Registry
        try:
            parsed_schema = self.get_parsed_schema(schema_id)
        except SchemaNotFoundError as exc:
            logger.warning("Schema ID %d not found for topic '%s': %s", schema_id, topic, exc)
            return DeserializedRecord(
                is_valid=False,
                schema_id=schema_id,
                event_id=None,
                event_type=None,
                event_timestamp_ms=None,
                payload_json=None,
                error_type="SCHEMA_NOT_FOUND",
                error_message=f"Schema ID {schema_id} not found in Schema Registry: {exc}",
                raw_payload_base64=raw_b64,
            )
        except SchemaRegistryError as exc:
            logger.error("Schema Registry communication failure for ID %d on topic '%s': %s", schema_id, topic, exc)
            return DeserializedRecord(
                is_valid=False,
                schema_id=schema_id,
                event_id=None,
                event_type=None,
                event_timestamp_ms=None,
                payload_json=None,
                error_type="SCHEMA_REGISTRY_ERROR",
                error_message=f"Failed to fetch schema ID {schema_id} from Schema Registry: {exc}",
                raw_payload_base64=raw_b64,
            )
        except Exception as exc:
            logger.error("Unexpected error retrieving schema ID %d: %s", schema_id, exc)
            return DeserializedRecord(
                is_valid=False,
                schema_id=schema_id,
                event_id=None,
                event_type=None,
                event_timestamp_ms=None,
                payload_json=None,
                error_type="SCHEMA_RESOLUTION_ERROR",
                error_message=f"Unexpected error resolving schema ID {schema_id}: {exc}",
                raw_payload_base64=raw_b64,
            )

        # Step 3: Decode Avro binary payload using fastavro
        try:
            decoded = fastavro.schemaless_reader(BytesIO(avro_payload), parsed_schema)
        except Exception as exc:
            logger.warning("Avro binary decode failure for schema ID %d on topic '%s': %s", schema_id, topic, exc)
            return DeserializedRecord(
                is_valid=False,
                schema_id=schema_id,
                event_id=None,
                event_type=None,
                event_timestamp_ms=None,
                payload_json=None,
                error_type="AVRO_DECODE_ERROR",
                error_message=f"Avro binary payload decoding failed: {exc}",
                raw_payload_base64=raw_b64,
            )

        # Step 4: Validate required structural envelope fields (event_id, event_type, timestamp)
        if not isinstance(decoded, dict):
            return DeserializedRecord(
                is_valid=False,
                schema_id=schema_id,
                event_id=None,
                event_type=None,
                event_timestamp_ms=None,
                payload_json=None,
                error_type="STRUCTURAL_VALIDATION_ERROR",
                error_message=f"Decoded Avro payload is not a dictionary: got {type(decoded).__name__}",
                raw_payload_base64=raw_b64,
            )

        event_id = decoded.get("event_id")
        event_type = decoded.get("event_type")
        timestamp_val = decoded.get("timestamp")

        if not event_id or not isinstance(event_id, str):
            return DeserializedRecord(
                is_valid=False,
                schema_id=schema_id,
                event_id=None,
                event_type=None,
                event_timestamp_ms=None,
                payload_json=None,
                error_type="STRUCTURAL_VALIDATION_ERROR",
                error_message="Missing or non-string required envelope field 'event_id'.",
                raw_payload_base64=raw_b64,
            )

        if not event_type or not isinstance(event_type, str):
            return DeserializedRecord(
                is_valid=False,
                schema_id=schema_id,
                event_id=event_id,
                event_type=None,
                event_timestamp_ms=None,
                payload_json=None,
                error_type="STRUCTURAL_VALIDATION_ERROR",
                error_message="Missing or non-string required envelope field 'event_type'.",
                raw_payload_base64=raw_b64,
            )

        if timestamp_val is None:
            return DeserializedRecord(
                is_valid=False,
                schema_id=schema_id,
                event_id=event_id,
                event_type=event_type,
                event_timestamp_ms=None,
                payload_json=None,
                error_type="STRUCTURAL_VALIDATION_ERROR",
                error_message="Missing required envelope field 'timestamp'.",
                raw_payload_base64=raw_b64,
            )

        # Step 5: Normalize timestamp to epoch milliseconds
        try:
            if isinstance(timestamp_val, datetime):
                dt = timestamp_val if timestamp_val.tzinfo is not None else timestamp_val.replace(tzinfo=UTC)
                event_timestamp_ms = int(dt.timestamp() * 1000)
            elif isinstance(timestamp_val, (int, float)):
                event_timestamp_ms = int(timestamp_val)
            elif isinstance(timestamp_val, str):
                dt = datetime.fromisoformat(timestamp_val.replace("Z", "+00:00"))
                dt = dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
                event_timestamp_ms = int(dt.timestamp() * 1000)
            else:
                raise ValueError(f"Unsupported timestamp data type: {type(timestamp_val).__name__}")
        except Exception as exc:
            return DeserializedRecord(
                is_valid=False,
                schema_id=schema_id,
                event_id=event_id,
                event_type=event_type,
                event_timestamp_ms=None,
                payload_json=None,
                error_type="STRUCTURAL_VALIDATION_ERROR",
                error_message=f"Failed to normalize 'timestamp' field ({timestamp_val!r}): {exc}",
                raw_payload_base64=raw_b64,
            )

        # Step 6: Encode clean dictionary into deterministic JSON string
        try:
            payload_json = json.dumps(decoded, default=_json_default_serializer, ensure_ascii=False)
        except Exception as exc:
            return DeserializedRecord(
                is_valid=False,
                schema_id=schema_id,
                event_id=event_id,
                event_type=event_type,
                event_timestamp_ms=event_timestamp_ms,
                payload_json=None,
                error_type="JSON_SERIALIZATION_ERROR",
                error_message=f"Failed to serialize decoded payload to JSON: {exc}",
                raw_payload_base64=raw_b64,
            )

        return DeserializedRecord(
            is_valid=True,
            schema_id=schema_id,
            event_id=event_id,
            event_type=event_type,
            event_timestamp_ms=event_timestamp_ms,
            payload_json=payload_json,
            error_type=None,
            error_message=None,
            raw_payload_base64=None,
        )


def get_deserialized_struct_type() -> StructType:
    """Return PySpark StructType schema corresponding to DeserializedRecord.

    Returns:
        StructType for deserialized record projection in PySpark.
    """
    return StructType(
        [
            StructField("is_valid", BooleanType(), nullable=False),
            StructField("schema_id", IntegerType(), nullable=True),
            StructField("event_id", StringType(), nullable=True),
            StructField("event_type", StringType(), nullable=True),
            StructField("event_timestamp_ms", LongType(), nullable=True),
            StructField("payload_json", StringType(), nullable=True),
            StructField("error_type", StringType(), nullable=True),
            StructField("error_message", StringType(), nullable=True),
            StructField("raw_payload_base64", StringType(), nullable=True),
        ]
    )


_WORKER_DESERIALIZER: ConfluentWireDeserializer | None = None


def _get_worker_deserializer(
    schema_registry_url: str,
    preloaded_schemas: dict[int, dict[str, Any]] | None = None,
) -> ConfluentWireDeserializer:
    """Obtain or initialize worker-cached singleton instance of ConfluentWireDeserializer."""
    global _WORKER_DESERIALIZER
    if _WORKER_DESERIALIZER is None:
        _WORKER_DESERIALIZER = ConfluentWireDeserializer(schema_registry_url=schema_registry_url)
        if preloaded_schemas:
            for schema_id, schema_dict in preloaded_schemas.items():
                _WORKER_DESERIALIZER.register_cached_schema(schema_id, schema_dict)
    return _WORKER_DESERIALIZER


def deserialize_kafka_stream(
    df: DataFrame,
    schema_registry_url: str,
    preloaded_schemas: dict[int, dict[str, Any]] | None = None,
) -> DataFrame:
    """Deserialize Kafka binary stream with Confluent wire format into structured DataFrame.

    Wraps ConfluentWireDeserializer in a PySpark UDF across DataFrame rows. Projects Kafka metadata
    along with expanded deserialized fields and error diagnostics.

    Args:
        df: Kafka streaming or batch DataFrame containing 'value' and 'topic' columns.
        schema_registry_url: Base URL for Confluent Schema Registry.
        preloaded_schemas: Optional dictionary of {schema_id: schema_dict} to seed worker cache.

    Returns:
        Transformed DataFrame containing metadata, valid event attributes, and error diagnostics.
    """

    @F.udf(returnType=get_deserialized_struct_type())
    def _deserialize_udf(payload_value: bytes | None, topic_value: str | None) -> dict[str, Any]:
        deserializer = _get_worker_deserializer(schema_registry_url, preloaded_schemas)
        record = deserializer.deserialize(payload_value, topic=topic_value)
        return record.to_dict()

    parsed_col = _deserialize_udf(F.col("value"), F.col("topic"))
    with_parsed = df.withColumn("_parsed", parsed_col)

    # Kafka metadata column resolution with fallbacks for synthetic testing
    key_col = F.col("key") if "key" in df.columns else F.lit(None).cast("binary").alias("key")
    val_col = F.col("value")
    top_col = F.col("topic") if "topic" in df.columns else F.lit("unknown").alias("topic")
    part_col = F.col("partition") if "partition" in df.columns else F.lit(0).alias("partition")
    off_col = F.col("offset") if "offset" in df.columns else F.lit(0).cast("long").alias("offset")

    if "timestamp" in df.columns:
        kafka_ts_col = F.col("timestamp").alias("kafka_timestamp")
    elif "kafka_timestamp" in df.columns:
        kafka_ts_col = F.col("kafka_timestamp")
    else:
        kafka_ts_col = F.current_timestamp().alias("kafka_timestamp")

    return with_parsed.select(
        key_col,
        val_col,
        top_col,
        part_col,
        off_col,
        kafka_ts_col,
        F.col("_parsed.is_valid").alias("is_valid"),
        F.col("_parsed.schema_id").alias("schema_id"),
        F.col("_parsed.event_id").alias("event_id"),
        F.col("_parsed.event_type").alias("event_type"),
        F.col("_parsed.event_timestamp_ms").alias("event_timestamp_ms"),
        F.col("_parsed.payload_json").alias("payload_json"),
        F.col("_parsed.error_type").alias("_error_type"),
        F.col("_parsed.error_message").alias("_error_message"),
        F.col("_parsed.raw_payload_base64").alias("_raw_payload_base64"),
    )
