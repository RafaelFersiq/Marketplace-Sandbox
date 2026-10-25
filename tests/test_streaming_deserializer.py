"""Unit tests for Confluent Avro wire-format deserializer and fault isolation.

Tests wire-format header unpacking (magic byte 0x00 + 4-byte big-endian schema ID),
dynamic schema resolution via cache and mocked Schema Registry, Avro payload decoding
across real marketplace schemas, structural envelope validation, and resilient error
tagging (poison pill isolation) without halting streaming pipelines.
"""

import base64
import json
import struct
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructType,
)

from ingestion.schema_registry import (
    SchemaNotFoundError,
    SchemaRegistryError,
    SchemaRegistryManager,
)
from ingestion.serialization import AvroSerializerDeserializer
from processing.streaming.deserializer import (
    ConfluentWireDeserializer,
    DeserializedRecord,
    WireFormatError,
    get_deserialized_struct_type,
)


@pytest.fixture
def schemas_dir() -> Path:
    """Return path to project Avro schemas directory."""
    return Path(__file__).resolve().parent.parent / "ingestion" / "schemas" / "avro"


@pytest.fixture
def avro_schemas(schemas_dir: Path) -> dict[str, dict[str, Any]]:
    """Load all 6 project Avro schemas parsed into dictionaries."""
    schemas: dict[str, dict[str, Any]] = {}
    for filepath in schemas_dir.glob("*.avsc"):
        schema_dict, _ = SchemaRegistryManager.load_schema_from_file(filepath)
        schemas[filepath.stem] = schema_dict
    return schemas


@pytest.fixture
def serializer() -> AvroSerializerDeserializer:
    """Return AvroSerializerDeserializer instance."""
    return AvroSerializerDeserializer()


@pytest.fixture
def sample_payloads() -> dict[str, dict[str, Any]]:
    """Return valid sample event payloads matching domain schemas."""
    return {
        "order_creation": {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "event_type": "ORDER_CREATED",
            "timestamp": datetime(2026, 7, 26, 12, 0, 0, tzinfo=UTC),
            "order_id": "ord-100",
            "customer_id": "cust-200",
            "seller_id": "sell-300",
            "order_status": "PENDING_PAYMENT",
            "currency": "BRL",
            "subtotal_amount": Decimal("100.0000"),
            "discount_amount": Decimal("0.0000"),
            "shipping_fee": Decimal("10.0000"),
            "total_amount": Decimal("110.0000"),
            "payment_method": "CREDIT_CARD",
            "shipping_address": {
                "street": "Rua Augusta",
                "number": "500",
                "complement": None,
                "city": "São Paulo",
                "state": "SP",
                "zip_code": "01305-000",
                "country": "BR",
            },
            "items": [
                {
                    "item_id": "item-1",
                    "product_id": "prod-1",
                    "product_name": "Keyboard",
                    "quantity": 1,
                    "unit_price": Decimal("100.0000"),
                    "subtotal": Decimal("100.0000"),
                }
            ],
        },
        "payment_updates": {
            "event_id": "e01104a3-2391-4e7a-8f81-550186981290",
            "event_type": "PAYMENT_STATUS_UPDATED",
            "timestamp": datetime(2026, 7, 26, 12, 1, 0, tzinfo=UTC),
            "payment_id": "pay-100",
            "order_id": "ord-100",
            "customer_id": "cust-200",
            "payment_method": "CREDIT_CARD",
            "payment_status": "APPROVED",
            "amount": Decimal("110.0000"),
            "currency": "BRL",
            "gateway_provider": "STRIPE",
            "transaction_reference": "txn-abc",
            "failure_reason": None,
            "installments": 1,
        },
        "user_navigation": {
            "event_id": "77128441-2990-11ec-abcd-001122334455",
            "event_type": "PAGE_VIEW",
            "timestamp": datetime(2026, 7, 26, 12, 2, 0, tzinfo=UTC),
            "session_id": "sess-500",
            "user_id": "cust-200",
            "device_type": "DESKTOP",
            "ip_address": "127.0.0.1",
            "user_agent": "Mozilla/5.0",
            "page_url": "https://marketplace.com/products/prod-1",
            "referrer_url": "https://marketplace.com",
            "product_id": "prod-1",
            "category_id": "cat-electronics",
            "search_term": None,
        },
        "cart_events": {
            "event_id": "91823ab1-1293-4819-aa29-881273619283",
            "event_type": "CART_ITEM_ADDED",
            "timestamp": datetime(2026, 7, 26, 12, 3, 0, tzinfo=UTC),
            "cart_id": "cart-100",
            "user_id": "cust-200",
            "session_id": "sess-500",
            "product_id": "prod-1",
            "quantity": 1,
            "unit_price": Decimal("100.0000"),
            "cart_total_items": 1,
            "cart_total_value": Decimal("100.0000"),
        },
        "stock_updates": {
            "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "event_type": "STOCK_RESERVED",
            "timestamp": datetime(2026, 7, 26, 12, 4, 0, tzinfo=UTC),
            "inventory_id": "inv-500",
            "product_id": "prod-1",
            "seller_id": "sell-300",
            "warehouse_id": "wh-sp-01",
            "quantity_change": -1,
            "previous_stock": 10,
            "new_stock": 9,
            "reason": "ORDER_RESERVATION",
            "order_id": "ord-100",
        },
        "catalog_changes": {
            "event_id": "c9981234-4412-9901-0012-aabbccddeeff",
            "event_type": "PRICE_CHANGED",
            "timestamp": datetime(2026, 7, 26, 12, 5, 0, tzinfo=UTC),
            "product_id": "prod-1",
            "seller_id": "sell-300",
            "sku": "KB-RGB-01",
            "product_name": "Mechanical Keyboard RGB",
            "category_id": "cat-elec",
            "brand": "TechKey",
            "price": Decimal("100.0000"),
            "previous_price": Decimal("120.0000"),
            "is_active": True,
            "updated_by": "admin-1",
        },
    }


class TestConfluentWireDeserializer:
    """Test suite for Confluent wire-format parsing, caching, and error isolation."""

    def test_wire_format_error_properties(self) -> None:
        err = WireFormatError("Header truncated", "TRUNCATED_HEADER")
        assert str(err) == "Header truncated"
        assert err.error_type == "TRUNCATED_HEADER"
        assert isinstance(err, ValueError)

    def test_deserialized_record_to_dict(self) -> None:
        record = DeserializedRecord(
            is_valid=True,
            schema_id=42,
            event_id="evt-100",
            event_type="ORDER_CREATED",
            event_timestamp_ms=1774612800000,
            payload_json='{"order_id": "ord-100"}',
            error_type=None,
            error_message=None,
            raw_payload_base64=None,
        )
        d = record.to_dict()
        assert d["is_valid"] is True
        assert d["schema_id"] == 42
        assert d["event_id"] == "evt-100"
        assert d["event_type"] == "ORDER_CREATED"
        assert d["event_timestamp_ms"] == 1774612800000
        assert d["payload_json"] == '{"order_id": "ord-100"}'
        assert d["error_type"] is None
        assert d["error_message"] is None
        assert d["raw_payload_base64"] is None

    def test_parse_wire_header_success(self) -> None:
        raw_header = b"\x00" + struct.pack(">I", 42) + b"sample_binary_payload"
        schema_id, avro_bytes = ConfluentWireDeserializer.parse_wire_header(raw_header)
        assert schema_id == 42
        assert avro_bytes == b"sample_binary_payload"

    def test_parse_wire_header_failures(self) -> None:
        with pytest.raises(WireFormatError, match=r"Payload is empty or None") as exc_none:
            ConfluentWireDeserializer.parse_wire_header(None)
        assert exc_none.value.error_type == "EMPTY_PAYLOAD"

        with pytest.raises(WireFormatError, match=r"Payload is empty or None") as exc_empty:
            ConfluentWireDeserializer.parse_wire_header(b"")
        assert exc_empty.value.error_type == "EMPTY_PAYLOAD"

        with pytest.raises(WireFormatError, match=r"shorter than Confluent header") as exc_trunc:
            ConfluentWireDeserializer.parse_wire_header(b"\x00\x01\x02")
        assert exc_trunc.value.error_type == "TRUNCATED_HEADER"

        with pytest.raises(WireFormatError, match=r"Invalid wire format magic byte") as exc_magic:
            ConfluentWireDeserializer.parse_wire_header(b"\x01\x00\x00\x00\x01payload")
        assert exc_magic.value.error_type == "INVALID_MAGIC_BYTE"

    def test_deserialize_valid_payloads_across_schemas(
        self,
        avro_schemas: dict[str, dict[str, Any]],
        sample_payloads: dict[str, dict[str, Any]],
        serializer: AvroSerializerDeserializer,
    ) -> None:
        deserializer = ConfluentWireDeserializer()
        schema_id_counter = 100

        for schema_stem, payload in sample_payloads.items():
            schema = avro_schemas[schema_stem]
            schema_id = schema_id_counter
            schema_id_counter += 1

            deserializer.register_cached_schema(schema_id, schema)

            raw_bytes = serializer.serialize(payload, schema, schema_id=schema_id, include_wire_header=True)
            record = deserializer.deserialize(raw_bytes, topic=f"marketplace.{schema_stem}")

            assert record.is_valid is True
            assert record.schema_id == schema_id
            assert record.event_id == payload["event_id"]
            assert record.event_type == payload["event_type"]
            assert isinstance(record.event_timestamp_ms, int)
            assert record.event_timestamp_ms > 0
            assert record.payload_json is not None
            assert record.error_type is None
            assert record.error_message is None
            assert record.raw_payload_base64 is None

            decoded_json = json.loads(record.payload_json)
            assert decoded_json["event_id"] == payload["event_id"]
            assert decoded_json["event_type"] == payload["event_type"]

    def test_schema_caching_and_fastavro_reuse(
        self,
        avro_schemas: dict[str, dict[str, Any]],
    ) -> None:
        mock_registry = MagicMock(spec=SchemaRegistryManager)
        deserializer = ConfluentWireDeserializer(schema_registry_manager=mock_registry)
        schema = avro_schemas["order_creation"]
        schema_id = 42

        deserializer.register_cached_schema(schema_id, schema)

        parsed_1 = deserializer.get_parsed_schema(schema_id)
        parsed_2 = deserializer.get_parsed_schema(schema_id)

        assert parsed_1 is parsed_2
        mock_registry.get_schema_by_id.assert_not_called()

    def test_remote_schema_registry_lookup(
        self,
        avro_schemas: dict[str, dict[str, Any]],
        sample_payloads: dict[str, dict[str, Any]],
        serializer: AvroSerializerDeserializer,
    ) -> None:
        mock_registry = MagicMock(spec=SchemaRegistryManager)
        schema = avro_schemas["payment_updates"]
        payload = sample_payloads["payment_updates"]
        schema_id = 88
        mock_registry.get_schema_by_id.return_value = schema

        deserializer = ConfluentWireDeserializer(schema_registry_manager=mock_registry)
        raw_bytes = serializer.serialize(payload, schema, schema_id=schema_id, include_wire_header=True)

        record = deserializer.deserialize(raw_bytes, topic="marketplace.payments")
        assert record.is_valid is True
        assert record.schema_id == schema_id
        assert record.event_id == payload["event_id"]
        mock_registry.get_schema_by_id.assert_called_once_with(schema_id)

        record_2 = deserializer.deserialize(raw_bytes, topic="marketplace.payments")
        assert record_2.is_valid is True
        mock_registry.get_schema_by_id.assert_called_once_with(schema_id)

    def test_error_isolation_empty_payload(self) -> None:
        deserializer = ConfluentWireDeserializer()

        rec_none = deserializer.deserialize(None)
        assert rec_none.is_valid is False
        assert rec_none.error_type == "EMPTY_PAYLOAD"
        assert rec_none.raw_payload_base64 == ""

        rec_empty = deserializer.deserialize(b"")
        assert rec_empty.is_valid is False
        assert rec_empty.error_type == "EMPTY_PAYLOAD"
        assert rec_empty.raw_payload_base64 == ""

    def test_error_isolation_truncated_header(self) -> None:
        deserializer = ConfluentWireDeserializer()
        raw_truncated = b"\x00\x01\x02"

        record = deserializer.deserialize(raw_truncated)
        assert record.is_valid is False
        assert record.error_type == "TRUNCATED_HEADER"
        assert record.raw_payload_base64 == base64.b64encode(raw_truncated).decode("ascii")

    def test_error_isolation_invalid_magic_byte(self) -> None:
        deserializer = ConfluentWireDeserializer()
        raw_invalid = b"\x01\x00\x00\x00\x01malformed"

        record = deserializer.deserialize(raw_invalid)
        assert record.is_valid is False
        assert record.error_type == "INVALID_MAGIC_BYTE"
        assert record.raw_payload_base64 == base64.b64encode(raw_invalid).decode("ascii")

    def test_error_isolation_schema_not_found(self) -> None:
        mock_registry = MagicMock(spec=SchemaRegistryManager)
        mock_registry.get_schema_by_id.side_effect = SchemaNotFoundError("Schema 999 does not exist")
        deserializer = ConfluentWireDeserializer(schema_registry_manager=mock_registry)

        raw_bytes = b"\x00" + struct.pack(">I", 999) + b"random_body"
        record = deserializer.deserialize(raw_bytes, topic="marketplace.orders")

        assert record.is_valid is False
        assert record.schema_id == 999
        assert record.error_type == "SCHEMA_NOT_FOUND"
        assert "Schema ID 999 not found" in str(record.error_message)
        assert record.raw_payload_base64 == base64.b64encode(raw_bytes).decode("ascii")

    def test_error_isolation_schema_registry_error(self) -> None:
        mock_registry = MagicMock(spec=SchemaRegistryManager)
        mock_registry.get_schema_by_id.side_effect = SchemaRegistryError("HTTP 503 Service Unavailable")
        deserializer = ConfluentWireDeserializer(schema_registry_manager=mock_registry)

        raw_bytes = b"\x00" + struct.pack(">I", 500) + b"some_body"
        record = deserializer.deserialize(raw_bytes, topic="marketplace.orders")

        assert record.is_valid is False
        assert record.schema_id == 500
        assert record.error_type == "SCHEMA_REGISTRY_ERROR"
        assert "Failed to fetch schema ID 500" in str(record.error_message)

    def test_error_isolation_corrupt_avro_binary_body(
        self,
        avro_schemas: dict[str, dict[str, Any]],
    ) -> None:
        deserializer = ConfluentWireDeserializer()
        schema_id = 55
        deserializer.register_cached_schema(schema_id, avro_schemas["cart_events"])

        raw_corrupt = b"\x00" + struct.pack(">I", schema_id) + b"\xff\xff\xff\xff\x00\x12"
        record = deserializer.deserialize(raw_corrupt, topic="marketplace.cart")

        assert record.is_valid is False
        assert record.schema_id == schema_id
        assert record.error_type == "AVRO_DECODE_ERROR"
        assert "Avro binary payload decoding failed" in str(record.error_message)
        assert record.raw_payload_base64 == base64.b64encode(raw_corrupt).decode("ascii")

    def test_error_isolation_missing_structural_envelope_fields(
        self,
        avro_schemas: dict[str, dict[str, Any]],
        sample_payloads: dict[str, dict[str, Any]],
        serializer: AvroSerializerDeserializer,
    ) -> None:
        deserializer = ConfluentWireDeserializer()
        schema = avro_schemas["order_creation"]
        schema_id = 70
        deserializer.register_cached_schema(schema_id, schema)

        payload_no_id = sample_payloads["order_creation"].copy()
        payload_no_id["event_id"] = ""
        raw_no_id = serializer.serialize(payload_no_id, schema, schema_id=schema_id, include_wire_header=True)
        rec_no_id = deserializer.deserialize(raw_no_id)
        assert rec_no_id.is_valid is False
        assert rec_no_id.error_type == "STRUCTURAL_VALIDATION_ERROR"
        assert "event_id" in str(rec_no_id.error_message)

        payload_no_type = sample_payloads["order_creation"].copy()
        payload_no_type["event_type"] = ""
        raw_no_type = serializer.serialize(payload_no_type, schema, schema_id=schema_id, include_wire_header=True)
        rec_no_type = deserializer.deserialize(raw_no_type)
        assert rec_no_type.is_valid is False
        assert rec_no_type.error_type == "STRUCTURAL_VALIDATION_ERROR"
        assert "event_type" in str(rec_no_type.error_message)

    def test_timestamp_normalization_formats(
        self,
        avro_schemas: dict[str, dict[str, Any]],
        sample_payloads: dict[str, dict[str, Any]],
        serializer: AvroSerializerDeserializer,
    ) -> None:
        deserializer = ConfluentWireDeserializer()
        schema = avro_schemas["user_navigation"]
        schema_id = 75
        deserializer.register_cached_schema(schema_id, schema)

        payload_dt = sample_payloads["user_navigation"].copy()
        payload_dt["timestamp"] = datetime(2026, 7, 26, 15, 30, 0, tzinfo=UTC)
        raw_dt = serializer.serialize(payload_dt, schema, schema_id=schema_id, include_wire_header=True)
        rec_dt = deserializer.deserialize(raw_dt)
        assert rec_dt.is_valid is True
        assert rec_dt.event_timestamp_ms == int(datetime(2026, 7, 26, 15, 30, 0, tzinfo=UTC).timestamp() * 1000)

    def test_get_deserialized_struct_type(self) -> None:
        struct_type = get_deserialized_struct_type()
        assert isinstance(struct_type, StructType)
        assert len(struct_type.fields) == 9

        field_map = {f.name: f.dataType for f in struct_type.fields}
        assert isinstance(field_map["is_valid"], BooleanType)
        assert isinstance(field_map["schema_id"], IntegerType)
        assert isinstance(field_map["event_id"], StringType)
        assert isinstance(field_map["event_type"], StringType)
        assert isinstance(field_map["event_timestamp_ms"], LongType)
        assert isinstance(field_map["payload_json"], StringType)
        assert isinstance(field_map["error_type"], StringType)
        assert isinstance(field_map["error_message"], StringType)
        assert isinstance(field_map["raw_payload_base64"], StringType)

    def test_preload_schemas_from_dir(self, schemas_dir: Path) -> None:
        deserializer = ConfluentWireDeserializer(schemas_dir=schemas_dir)
        assert deserializer.schemas_dir == schemas_dir

        deserializer.preload_schemas_from_dir(Path("/tmp/non_existent_schemas_path_abc123"))
