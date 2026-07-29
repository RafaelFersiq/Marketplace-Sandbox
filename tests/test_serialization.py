"""Unit tests for Avro payload serialization, deserialization, and schema validation.

Validates payload contract compliance, binary encoding/decoding, Confluent wire format
headers, and custom exception handling across all 6 business event types with support for
timestamp-millis (datetime) and decimal (Decimal) logical types.
"""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from ingestion.schema_registry import SchemaRegistryManager
from ingestion.serialization import (
    AvroSerializerDeserializer,
    DeserializationError,
    SchemaValidationError,
    SerializationError,
)


@pytest.fixture
def schemas_dir() -> Path:
    """Return path to Avro schemas directory."""
    return Path(__file__).resolve().parent.parent / "ingestion" / "schemas" / "avro"


@pytest.fixture
def avro_schemas(schemas_dir: Path) -> dict[str, dict[str, Any]]:
    """Load all 6 parsed Avro schemas into a dictionary keyed by event stem."""
    schemas = {}
    for filepath in schemas_dir.glob("*.avsc"):
        stem = filepath.stem
        schema_dict, _ = SchemaRegistryManager.load_schema_from_file(filepath)
        schemas[stem] = schema_dict
    return schemas


@pytest.fixture
def sample_events() -> dict[str, dict[str, Any]]:
    """Return valid sample event payloads for all 6 business event types."""
    return {
        "cart_events": {
            "event_id": "91823ab1-1293-4819-aa29-881273619283",
            "event_type": "CART_ITEM_ADDED",
            "timestamp": datetime(2026, 7, 26, 17, 20, 12, 450000, tzinfo=UTC),
            "cart_id": "cart-991283",
            "user_id": "cust-10293",
            "session_id": "sess-88192-3312",
            "product_id": "prod-77123",
            "quantity": 2,
            "unit_price": Decimal("125.0000"),
            "cart_total_items": 3,
            "cart_total_value": Decimal("375.0000"),
        },
        "catalog_changes": {
            "event_id": "c9981234-4412-9901-0012-aabbccddeeff",
            "event_type": "PRICE_CHANGED",
            "timestamp": datetime(2026, 7, 26, 16, 0, 0, 0, tzinfo=UTC),
            "product_id": "prod-77123",
            "seller_id": "sell-99201",
            "sku": "AUDIO-HD-WIFI-01",
            "product_name": "Wireless Headphones Noise Cancelling",
            "category_id": "cat-elec-01",
            "brand": "AudioTech",
            "price": Decimal("125.0000"),
            "previous_price": Decimal("149.9900"),
            "is_active": True,
            "updated_by": "user-admin-04",
        },
        "order_creation": {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "event_type": "ORDER_CREATED",
            "timestamp": datetime(2026, 7, 26, 17, 25, 0, 0, tzinfo=UTC),
            "order_id": "ord-88392-4920",
            "customer_id": "cust-10293",
            "seller_id": "sell-99201",
            "order_status": "PENDING_PAYMENT",
            "currency": "BRL",
            "subtotal_amount": Decimal("250.0000"),
            "discount_amount": Decimal("10.0000"),
            "shipping_fee": Decimal("15.0000"),
            "total_amount": Decimal("255.0000"),
            "payment_method": "PIX",
            "shipping_address": {
                "street": "Av. Paulista",
                "number": "1000",
                "complement": "Apt 42",
                "city": "São Paulo",
                "state": "SP",
                "zip_code": "01310-100",
                "country": "BR",
            },
            "items": [
                {
                    "item_id": "item-001",
                    "product_id": "prod-77123",
                    "product_name": "Wireless Headphones",
                    "quantity": 2,
                    "unit_price": Decimal("125.0000"),
                    "subtotal": Decimal("250.0000"),
                }
            ],
        },
        "payment_updates": {
            "event_id": "e01104a3-2391-4e7a-8f81-550186981290",
            "event_type": "PAYMENT_STATUS_UPDATED",
            "timestamp": datetime(2026, 7, 26, 17, 25, 5, 120000, tzinfo=UTC),
            "payment_id": "pay-44910-8812",
            "order_id": "ord-88392-4920",
            "customer_id": "cust-10293",
            "payment_method": "PIX",
            "payment_status": "APPROVED",
            "amount": Decimal("255.0000"),
            "currency": "BRL",
            "gateway_provider": "MOCK_GATEWAY",
            "transaction_reference": "tx_pix_991823712",
            "failure_reason": None,
            "installments": 1,
        },
        "stock_updates": {
            "event_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "event_type": "STOCK_RESERVED",
            "timestamp": datetime(2026, 7, 26, 17, 25, 1, 0, tzinfo=UTC),
            "inventory_id": "inv-55102",
            "product_id": "prod-77123",
            "seller_id": "sell-99201",
            "warehouse_id": "wh-sp-01",
            "quantity_change": -2,
            "previous_stock": 50,
            "new_stock": 48,
            "reason": "ORDER_RESERVATION",
            "order_id": "ord-88392-4920",
        },
        "user_navigation": {
            "event_id": "77128441-2990-11ec-abcd-001122334455",
            "event_type": "PRODUCT_CLICK",
            "timestamp": datetime(2026, 7, 26, 17, 18, 40, 0, tzinfo=UTC),
            "session_id": "sess-88192-3312",
            "user_id": "cust-10293",
            "device_type": "MOBILE",
            "ip_address": "189.100.22.15",
            "user_agent": "Mozilla/5.0 (iPhone; CPU OS 17_5 like Mac OS X)",
            "page_url": "https://marketplace.com/products/prod-77123",
            "referrer_url": "https://marketplace.com/categories/electronics",
            "product_id": "prod-77123",
            "category_id": "cat-elec-01",
            "search_term": None,
        },
    }


def test_schema_validation_success_all_events(
    avro_schemas: dict[str, dict[str, Any]],
    sample_events: dict[str, dict[str, Any]],
) -> None:
    """Verify schema validation succeeds for all 6 sample event types."""
    serde = AvroSerializerDeserializer()

    for event_name, payload in sample_events.items():
        schema = avro_schemas[event_name]
        assert serde.validate(payload, schema) is True


def test_schema_validation_failure_missing_required_field(
    avro_schemas: dict[str, dict[str, Any]],
    sample_events: dict[str, dict[str, Any]],
) -> None:
    """Verify validation fails when a required field is missing."""
    serde = AvroSerializerDeserializer()
    schema = avro_schemas["order_creation"]
    invalid_payload = sample_events["order_creation"].copy()
    del invalid_payload["order_id"]

    with pytest.raises(SchemaValidationError) as exc_info:
        serde.validate(invalid_payload, schema)

    assert "order_id" in str(exc_info.value) or "order_creation" in str(exc_info.value)


def test_schema_validation_failure_invalid_type(
    avro_schemas: dict[str, dict[str, Any]],
    sample_events: dict[str, dict[str, Any]],
) -> None:
    """Verify validation fails when a field has an invalid data type."""
    serde = AvroSerializerDeserializer()
    schema = avro_schemas["cart_events"]
    invalid_payload = sample_events["cart_events"].copy()
    invalid_payload["cart_total_items"] = "not_an_int"  # Expected int

    with pytest.raises(SchemaValidationError):
        serde.validate(invalid_payload, schema)


def test_raw_avro_serialization_deserialization_roundtrip(
    avro_schemas: dict[str, dict[str, Any]],
    sample_events: dict[str, dict[str, Any]],
) -> None:
    """Test raw Avro serialization and deserialization (without wire format header)."""
    serde = AvroSerializerDeserializer()

    for event_name, payload in sample_events.items():
        schema = avro_schemas[event_name]
        encoded = serde.serialize(payload, schema, include_wire_header=False)
        assert isinstance(encoded, bytes)

        schema_id, decoded = serde.deserialize(encoded, reader_schema=schema, has_wire_header=False)
        assert schema_id is None
        assert decoded["event_id"] == payload["event_id"]
        assert isinstance(decoded["timestamp"], datetime)


def test_confluent_wire_format_serialization_deserialization(
    avro_schemas: dict[str, dict[str, Any]],
    sample_events: dict[str, dict[str, Any]],
) -> None:
    """Test Avro serialization/deserialization with 5-byte Confluent wire format header."""
    serde = AvroSerializerDeserializer()
    expected_schema_id = 42

    for event_name, payload in sample_events.items():
        schema = avro_schemas[event_name]
        encoded = serde.serialize(payload, schema, schema_id=expected_schema_id, include_wire_header=True)

        assert isinstance(encoded, bytes)
        assert len(encoded) > 5
        assert encoded[0] == 0  # Magic byte

        schema_id, decoded = serde.deserialize(encoded, reader_schema=schema, has_wire_header=True)
        assert schema_id == expected_schema_id
        assert decoded["event_id"] == payload["event_id"]
        assert isinstance(decoded["timestamp"], datetime)


def test_serialization_missing_schema_id_with_wire_header(
    avro_schemas: dict[str, dict[str, Any]],
    sample_events: dict[str, dict[str, Any]],
) -> None:
    """Verify SerializationError is raised if schema_id is missing with wire header."""
    serde = AvroSerializerDeserializer()
    schema = avro_schemas["cart_events"]
    payload = sample_events["cart_events"]

    with pytest.raises(SerializationError) as exc_info:
        serde.serialize(payload, schema, schema_id=None, include_wire_header=True)

    assert "schema_id must be provided" in str(exc_info.value)


def test_deserialization_invalid_header_magic_byte() -> None:
    """Verify DeserializationError is raised when wire header magic byte is invalid."""
    serde = AvroSerializerDeserializer()
    invalid_data = b"\x01\x00\x00\x00\x01somepayload"

    with pytest.raises(DeserializationError) as exc_info:
        serde.deserialize(invalid_data, has_wire_header=True)

    assert "Invalid magic byte" in str(exc_info.value)


def test_deserialization_truncated_header() -> None:
    """Verify DeserializationError is raised when data length is shorter than 5 bytes."""
    serde = AvroSerializerDeserializer()
    short_data = b"\x00\x00\x01"

    with pytest.raises(DeserializationError) as exc_info:
        serde.deserialize(short_data, has_wire_header=True)

    assert "shorter than header size" in str(exc_info.value)


def test_deserialization_with_schema_registry_lookup(
    avro_schemas: dict[str, dict[str, Any]],
    sample_events: dict[str, dict[str, Any]],
) -> None:
    """Verify deserialization resolves schema dynamically via SchemaRegistryManager when reader_schema is None."""
    mock_registry = MagicMock(spec=SchemaRegistryManager)
    schema = avro_schemas["order_creation"]
    payload = sample_events["order_creation"]
    target_id = 99

    mock_registry.get_schema_by_id.return_value = schema

    serde = AvroSerializerDeserializer(schema_registry_manager=mock_registry)
    encoded = serde.serialize(payload, schema, schema_id=target_id, include_wire_header=True)

    # Deserialize without passing reader_schema explicitly
    schema_id, decoded = serde.deserialize(encoded, reader_schema=None, has_wire_header=True)

    assert schema_id == target_id
    assert decoded["event_id"] == payload["event_id"]
    assert isinstance(decoded["timestamp"], datetime)
    assert isinstance(decoded["total_amount"], Decimal)
    mock_registry.get_schema_by_id.assert_called_once_with(target_id)
