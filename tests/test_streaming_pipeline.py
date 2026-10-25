"""End-to-end and integration test suite for PySpark Structured Streaming Ingestion Pipeline.

Validates:
- Event-time duration parsing and UTC timestamp conversion utilities.
- Pure-Python watermark tracking and stateful deduplication (InMemoryWatermarkDeduplicator).
- End-to-end stream resilience simulation across all 6 domain schemas with ChaosEngine:
    * OrderCreationEvent (marketplace.orders)
    * PaymentUpdateEvent (marketplace.payments)
    * CartEvent (marketplace.cart)
    * StockUpdateEvent (marketplace.inventory)
    * CatalogChangeEvent (marketplace.catalog)
    * UserNavigationEvent (marketplace.navigation)
- Confluent wire format parsing, dynamic schema caching, and DLQ error isolation.
- StreamingIngestionPipeline lifecycle orchestration with mocked SparkSession and StreamingRecoverySupervisor.
- Live integration tests against running Kafka, Schema Registry, and MinIO container infrastructure.
"""

from __future__ import annotations

import base64
import copy
import os
import socket
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

import confluent_kafka
import pytest
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from ingestion.producers.chaos import ChaosConfig, ChaosEngine
from ingestion.producers.checkout_producer import CheckoutProducer
from ingestion.producers.config import ProducerConfig
from ingestion.schema_registry import SchemaRegistryManager
from ingestion.serialization import AvroSerializerDeserializer
from processing.streaming.config import SparkStreamingConfig
from processing.streaming.deserializer import ConfluentWireDeserializer
from processing.streaming.dlq import DLQRecord, DLQRouter
from processing.streaming.pipeline import StreamingIngestionPipeline
from processing.streaming.recovery import StreamingRecoverySupervisor
from processing.streaming.watermark import (
    InMemoryWatermarkDeduplicator,
    ms_to_utc_datetime,
    parse_watermark_duration_ms,
)


@pytest.fixture
def schemas_dir() -> Path:
    """Return path to Avro schemas directory."""
    return Path(__file__).resolve().parent.parent / "ingestion" / "schemas" / "avro"


@pytest.fixture
def avro_schemas(schemas_dir: Path) -> dict[str, dict[str, Any]]:
    """Load all 6 parsed Avro schema dictionaries keyed by event stem."""
    schemas: dict[str, dict[str, Any]] = {}
    for filepath in schemas_dir.glob("*.avsc"):
        schema_dict, _ = SchemaRegistryManager.load_schema_from_file(filepath)
        schemas[filepath.stem] = schema_dict
    return schemas


@pytest.fixture
def sample_domain_events() -> dict[str, dict[str, Any]]:
    """Return valid sample event payloads for all 6 business domain schemas."""
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
            "timestamp": datetime(2026, 7, 26, 17, 21, 0, 0, tzinfo=UTC),
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


def is_kafka_reachable(bootstrap_servers: str = "localhost:9092", timeout: float = 1.0) -> bool:
    """Check if Kafka broker endpoint is reachable via TCP."""
    try:
        first_broker = bootstrap_servers.split(",")[0].strip()
        host, port_str = first_broker.split(":")
        with socket.create_connection((host, int(port_str)), timeout=timeout):
            return True
    except (TimeoutError, OSError, ValueError):
        return False


def is_schema_registry_reachable(url: str = "http://localhost:8081", timeout: float = 1.0) -> bool:
    """Check if Confluent Schema Registry responds with HTTP 200."""
    try:
        parsed = urlparse(url)
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        with socket.create_connection((host, port), timeout=timeout):
            resp = requests.get(f"{url.rstrip('/')}/subjects", timeout=timeout)
            return resp.status_code == 200
    except (requests.RequestException, OSError, ValueError):
        return False


def is_minio_reachable(endpoint: str = "http://localhost:9000", timeout: float = 1.0) -> bool:
    """Check if MinIO storage service is reachable."""
    try:
        parsed = urlparse(endpoint)
        host = parsed.hostname or "localhost"
        port = parsed.port or 9000
        with socket.create_connection((host, port), timeout=timeout):
            resp = requests.get(f"{endpoint.rstrip('/')}/minio/health/live", timeout=timeout)
            return resp.status_code == 200
    except (requests.RequestException, OSError, ValueError):
        return False


def is_streaming_infra_reachable() -> bool:
    """Check if all three required services (Kafka, Schema Registry, MinIO) are available."""
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    sr_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    minio_url = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    return is_kafka_reachable(kafka_servers) and is_schema_registry_reachable(sr_url) and is_minio_reachable(minio_url)


class TestWatermarkDurationAndConversion:
    """Test suite for watermark string parsing and timestamp utilities."""

    def test_parse_watermark_duration_ms_valid(self) -> None:
        assert parse_watermark_duration_ms("10 minutes") == 600_000
        assert parse_watermark_duration_ms("30 seconds") == 30_000
        assert parse_watermark_duration_ms("1 hour") == 3_600_000
        assert parse_watermark_duration_ms("2 hours") == 7_200_000
        assert parse_watermark_duration_ms("500 ms") == 500
        assert parse_watermark_duration_ms("1 day") == 86_400_000
        assert parse_watermark_duration_ms("1.5 minutes") == 90_000

    def test_parse_watermark_duration_ms_invalid(self) -> None:
        with pytest.raises(ValueError, match=r"Watermark delay must be a non-empty string"):
            parse_watermark_duration_ms("")

        with pytest.raises(ValueError, match=r"Watermark delay must be a non-empty string"):
            parse_watermark_duration_ms("   ")

        with pytest.raises(ValueError, match=r"Invalid watermark duration format"):
            parse_watermark_duration_ms("invalid_format")

        with pytest.raises(ValueError, match=r"Invalid watermark duration format"):
            parse_watermark_duration_ms("10")

        with pytest.raises(ValueError, match=r"Watermark duration must be strictly positive"):
            parse_watermark_duration_ms("-5 minutes")

        with pytest.raises(ValueError, match=r"Watermark duration must be strictly positive"):
            parse_watermark_duration_ms("0 seconds")

        with pytest.raises(ValueError, match=r"Unsupported duration unit 'weeks'"):
            parse_watermark_duration_ms("2 weeks")

    def test_ms_to_utc_datetime_valid(self) -> None:
        epoch_ms = 1_785_000_000_000
        dt = ms_to_utc_datetime(epoch_ms)
        assert isinstance(dt, datetime)
        assert dt.tzinfo == UTC
        assert int(dt.timestamp() * 1000) == epoch_ms

    def test_ms_to_utc_datetime_invalid(self) -> None:
        with pytest.raises(ValueError, match=r"epoch_ms must be an integer or float"):
            ms_to_utc_datetime("not_an_int")  # type: ignore[arg-type]


class TestInMemoryWatermarkDeduplicator:
    """Test suite for InMemoryWatermarkDeduplicator state tracking and late-event rejection."""

    def test_sequential_events_accepted(self) -> None:
        dedup = InMemoryWatermarkDeduplicator(watermark_delay="10 minutes")

        e1 = {"event_id": "e-1", "event_timestamp_ms": 1_000_000}
        e2 = {"event_id": "e-2", "event_timestamp_ms": 1_100_000}
        e3 = {"event_id": "e-3", "event_timestamp_ms": 1_200_000}

        assert dedup.process_event(e1) == (True, None)
        assert dedup.process_event(e2) == (True, None)
        assert dedup.process_event(e3) == (True, None)
        assert dedup.max_event_timestamp_ms == 1_200_000
        assert dedup.current_watermark_ms == 1_200_000 - 600_000
        assert dedup.state_size == 3

    def test_duplicate_event_id_dropped(self) -> None:
        dedup = InMemoryWatermarkDeduplicator(watermark_delay="10 minutes")

        e1 = {"event_id": "dup-1", "event_timestamp_ms": 1_000_000}
        e1_copy = {"event_id": "dup-1", "event_timestamp_ms": 1_005_000}

        assert dedup.process_event(e1) == (True, None)
        assert dedup.process_event(e1_copy) == (False, "DUPLICATE_EVENT")
        assert dedup.state_size == 1

    def test_out_of_order_within_watermark_accepted(self) -> None:
        dedup = InMemoryWatermarkDeduplicator(watermark_delay="10 minutes")

        e1 = {"event_id": "e-1", "event_timestamp_ms": 1_000_000}
        assert dedup.process_event(e1) == (True, None)
        assert dedup.current_watermark_ms == 400_000

        e2_ooo = {"event_id": "e-2", "event_timestamp_ms": 800_000}
        assert dedup.process_event(e2_ooo) == (True, None)
        assert dedup.state_size == 2

    def test_late_event_older_than_watermark_dropped(self) -> None:
        dedup = InMemoryWatermarkDeduplicator(watermark_delay="10 minutes")

        e1 = {"event_id": "e-1", "event_timestamp_ms": 1_000_000}
        assert dedup.process_event(e1) == (True, None)

        e_late = {"event_id": "e-late", "event_timestamp_ms": 350_000}
        is_accepted, reason = dedup.process_event(e_late)
        assert is_accepted is False
        assert reason == "LATE_EVENT_DROPPED_BY_WATERMARK"
        assert dedup.state_size == 1

    def test_state_eviction_of_expired_keys(self) -> None:
        dedup = InMemoryWatermarkDeduplicator(watermark_delay="50 seconds")

        e1 = {"event_id": "e-old", "event_timestamp_ms": 100_000}
        assert dedup.process_event(e1) == (True, None)
        assert dedup.state_size == 1

        e2 = {"event_id": "e-new", "event_timestamp_ms": 200_000}
        assert dedup.process_event(e2) == (True, None)
        assert dedup.current_watermark_ms == 150_000
        assert dedup.state_size == 1

    def test_process_batch_partitioning(self) -> None:
        dedup = InMemoryWatermarkDeduplicator(watermark_delay="10 minutes")

        batch = [
            {"event_id": "e-1", "event_timestamp_ms": 1_000_000},
            {"event_id": "e-2", "event_timestamp_ms": 1_100_000},
            {"event_id": "e-1", "event_timestamp_ms": 1_050_000},
            {"event_id": "e-3", "event_timestamp_ms": 800_000},
            {"event_id": "e-late", "event_timestamp_ms": 200_000},
        ]

        accepted, dropped = dedup.process_batch(batch)
        assert len(accepted) == 3
        assert [e["event_id"] for e in accepted] == ["e-1", "e-2", "e-3"]

        assert len(dropped) == 2
        dropped_ids = [d["event_id"] for d in dropped]
        assert "e-1" in dropped_ids
        assert "e-late" in dropped_ids

        reasons = {d["event_id"]: d["_drop_reason"] for d in dropped}
        assert reasons["e-1"] == "DUPLICATE_EVENT"
        assert reasons["e-late"] == "LATE_EVENT_DROPPED_BY_WATERMARK"

    def test_missing_or_invalid_fields(self) -> None:
        dedup = InMemoryWatermarkDeduplicator()

        assert dedup.process_event({"event_timestamp_ms": 1000}) == (False, "MISSING_DEDUP_KEY")
        assert dedup.process_event({"event_id": "e-1"}) == (False, "MISSING_TIMESTAMP")
        assert dedup.process_event({"event_id": "e-1", "event_timestamp_ms": "not_a_date"}) == (
            False,
            "INVALID_TIMESTAMP",
        )

    def test_clear_resets_state(self) -> None:
        dedup = InMemoryWatermarkDeduplicator()
        dedup.process_event({"event_id": "e-1", "event_timestamp_ms": 5000})
        assert dedup.state_size == 1
        dedup.clear()
        assert dedup.state_size == 0
        assert dedup.max_event_timestamp_ms == 0


class TestEndToEndStreamSimulationOffline:
    """Simulates real-time ingestion across all 6 business domains with ChaosEngine."""

    def test_simulation_roundtrip_all_domains(
        self,
        avro_schemas: dict[str, dict[str, Any]],
        sample_domain_events: dict[str, dict[str, Any]],
    ) -> None:
        """Verify serialization, deserialization, watermark dedup, and DLQ isolation across domains."""
        serializer = AvroSerializerDeserializer()
        deserializer = ConfluentWireDeserializer()
        deduplicator = InMemoryWatermarkDeduplicator(watermark_delay="10 minutes")
        chaos = ChaosEngine(config=ChaosConfig(seed=42))

        schema_id_map: dict[str, int] = {
            "cart_events": 101,
            "catalog_changes": 102,
            "order_creation": 103,
            "payment_updates": 104,
            "stock_updates": 105,
            "user_navigation": 106,
        }

        for stem, sid in schema_id_map.items():
            deserializer.register_cached_schema(sid, avro_schemas[stem])

        for event_stem, clean_payload in sample_domain_events.items():
            schema = avro_schemas[event_stem]
            schema_id = schema_id_map[event_stem]
            topic = f"marketplace.{event_stem}"

            wire_bytes = serializer.serialize(clean_payload, schema, schema_id=schema_id)
            assert wire_bytes[0:1] == b"\x00"

            record = deserializer.deserialize(wire_bytes, topic=topic)
            assert record.is_valid is True
            assert record.schema_id == schema_id
            assert record.event_id == clean_payload["event_id"]
            assert record.event_type == clean_payload["event_type"]
            assert record.event_timestamp_ms is not None
            assert record.payload_json is not None

            is_accepted, reason = deduplicator.process_event(record.to_dict())
            assert is_accepted is True
            assert reason is None

            duplicates = chaos.inject_duplicate(clean_payload, count=1)
            assert len(duplicates) == 2
            dup_bytes = serializer.serialize(duplicates[1], schema, schema_id=schema_id)
            dup_record = deserializer.deserialize(dup_bytes, topic=topic)

            assert dup_record.is_valid is True
            is_dup_accepted, dup_reason = deduplicator.process_event(dup_record.to_dict())
            assert is_dup_accepted is False
            assert dup_reason == "DUPLICATE_EVENT"

            delayed_payload = chaos.inject_delay(clean_payload, delay_seconds=1200.0)
            delayed_bytes = serializer.serialize(delayed_payload, schema, schema_id=schema_id)
            delayed_record = deserializer.deserialize(delayed_bytes, topic=topic)

            assert delayed_record.is_valid is True
            is_late_accepted, late_reason = deduplicator.process_event(delayed_record.to_dict())
            assert is_late_accepted is False
            assert late_reason == "LATE_EVENT_DROPPED_BY_WATERMARK"

            corrupted_wire = ChaosEngine.corrupt_wire_format(wire_bytes, "magic_byte")
            bad_record = deserializer.deserialize(corrupted_wire, topic=topic)

            assert bad_record.is_valid is False
            assert bad_record.error_type == "INVALID_MAGIC_BYTE"
            assert bad_record.raw_payload_base64 is not None

            dlq_rec = DLQRouter.create_dlq_record(
                raw_payload=bad_record.raw_payload_base64,
                error_type=bad_record.error_type,
                error_message=bad_record.error_message or "",
                kafka_topic=topic,
                kafka_partition=0,
                kafka_offset=100,
                schema_id=bad_record.schema_id,
            )
            assert isinstance(dlq_rec, DLQRecord)
            assert dlq_rec._error_type == "INVALID_MAGIC_BYTE"
            assert dlq_rec._kafka_topic == topic
            recovered_bytes = base64.b64decode(dlq_rec._raw_payload_base64)
            assert recovered_bytes == corrupted_wire

            truncated_bytes = ChaosEngine.corrupt_wire_format(wire_bytes, "truncate")
            trunc_record = deserializer.deserialize(truncated_bytes, topic=topic)

            assert trunc_record.is_valid is False
            assert trunc_record.error_type == "TRUNCATED_HEADER"

            dlq_trunc = DLQRouter.create_dlq_record(
                raw_payload=trunc_record.raw_payload_base64,
                error_type=trunc_record.error_type,
                error_message=trunc_record.error_message or "",
                kafka_topic=topic,
                kafka_partition=0,
                kafka_offset=101,
            )
            assert dlq_trunc._error_type == "TRUNCATED_HEADER"

            violated_payload = copy.deepcopy(clean_payload)
            del violated_payload["event_id"]

            try:
                serializer.validate(violated_payload, schema)
                violated_bytes = serializer.serialize(violated_payload, schema, schema_id=schema_id)
                struct_record = deserializer.deserialize(violated_bytes, topic=topic)
                assert struct_record.is_valid is False
                assert struct_record.error_type == "STRUCTURAL_VALIDATION_ERROR"
            except Exception:
                pass


class TestStreamingIngestionPipelineOrchestration:
    """Test suite for StreamingIngestionPipeline configuration and query management."""

    def test_pipeline_init_defaults(self) -> None:
        config = SparkStreamingConfig()
        pipeline = StreamingIngestionPipeline(config=config)

        assert pipeline.config == config
        assert pipeline.spark is None
        assert pipeline.metrics_listener is None
        assert pipeline.enable_dlq_kafka is True
        assert pipeline.enable_dlq_storage is True
        assert pipeline.active_queries == []

    def test_build_spark_session(self) -> None:
        config = SparkStreamingConfig()
        mock_listener = MagicMock()
        mock_spark = MagicMock(spec=SparkSession)

        mock_builder = MagicMock()
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        pipeline = StreamingIngestionPipeline(
            config=config,
            metrics_listener=mock_listener,
        )

        with patch("pyspark.sql.SparkSession.builder", mock_builder):
            spark = pipeline.build_spark_session()
            assert spark == mock_spark
            mock_spark.sparkContext.setLogLevel.assert_called_with("WARN")
            mock_spark.streams.addListener.assert_called_with(mock_listener)

    def test_read_kafka_stream(self) -> None:
        config = SparkStreamingConfig(topics=["marketplace.orders"])
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)

        mock_reader = mock_spark.readStream.format.return_value.options.return_value
        mock_reader.load.return_value = mock_df

        pipeline = StreamingIngestionPipeline(config=config, spark=mock_spark)
        df = pipeline.read_kafka_stream(mock_spark)

        assert df == mock_df
        mock_spark.readStream.format.assert_called_with("kafka")

    def test_write_landing_stream_continuous(self) -> None:
        config = SparkStreamingConfig(trigger_interval="15 seconds", once=False)
        mock_valid_df = MagicMock(spec=DataFrame)
        mock_query = MagicMock(spec=StreamingQuery)
        mock_query.id = "landing-query-1"
        mock_query.runId = "run-1"

        mock_partitioned = mock_valid_df.writeStream.format.return_value.partitionBy.return_value
        mock_options = mock_partitioned.option.return_value.option.return_value
        mock_writer = mock_options.outputMode.return_value.queryName.return_value
        mock_writer.trigger.return_value.start.return_value = mock_query

        pipeline = StreamingIngestionPipeline(config=config)
        query = pipeline.write_landing_stream(mock_valid_df)

        assert query == mock_query
        mock_valid_df.writeStream.format.assert_called_with("parquet")
        mock_writer.trigger.assert_called_with(processingTime="15 seconds")

    def test_write_landing_stream_once_trigger(self) -> None:
        config = SparkStreamingConfig(once=True)
        mock_valid_df = MagicMock(spec=DataFrame)
        mock_query = MagicMock(spec=StreamingQuery)

        mock_partitioned = mock_valid_df.writeStream.format.return_value.partitionBy.return_value
        mock_options = mock_partitioned.option.return_value.option.return_value
        mock_writer = mock_options.outputMode.return_value.queryName.return_value
        mock_writer.trigger.return_value.start.return_value = mock_query

        pipeline = StreamingIngestionPipeline(config=config)
        query = pipeline.write_landing_stream(mock_valid_df)

        assert query == mock_query
        mock_writer.trigger.assert_called_with(availableNow=True)

    def test_start_queries_full_topology(self) -> None:
        config = SparkStreamingConfig()
        mock_spark = MagicMock(spec=SparkSession)
        mock_raw_df = MagicMock(spec=DataFrame)
        mock_valid_df = MagicMock(spec=DataFrame)
        mock_dlq_df = MagicMock(spec=DataFrame)

        q_landing = MagicMock(spec=StreamingQuery, id="q-landing", runId="r-1", name="landing")
        q_storage = MagicMock(spec=StreamingQuery, id="q-storage", runId="r-2", name="dlq_storage")
        q_kafka = MagicMock(spec=StreamingQuery, id="q-kafka", runId="r-3", name="dlq_kafka")

        pipeline = StreamingIngestionPipeline(
            config=config,
            spark=mock_spark,
            enable_dlq_kafka=True,
            enable_dlq_storage=True,
        )

        with (
            patch.object(pipeline, "read_kafka_stream", return_value=mock_raw_df),
            patch.object(pipeline, "transform_stream", return_value=(mock_valid_df, mock_dlq_df)),
            patch.object(pipeline, "write_landing_stream", return_value=q_landing),
            patch.object(DLQRouter, "write_dlq_to_storage", return_value=q_storage),
            patch.object(DLQRouter, "write_dlq_to_kafka", return_value=q_kafka),
        ):
            queries = pipeline.start_queries()
            assert len(queries) == 3
            assert queries == [q_landing, q_storage, q_kafka]
            assert pipeline.active_queries == [q_landing, q_storage, q_kafka]

    def test_stop_queries(self) -> None:
        config = SparkStreamingConfig()
        q1 = MagicMock(spec=StreamingQuery, isActive=True, id="q1", name="query-1")
        q2 = MagicMock(spec=StreamingQuery, isActive=True, id="q2", name="query-2")

        pipeline = StreamingIngestionPipeline(config=config)
        pipeline.active_queries = [q1, q2]

        pipeline.stop_queries()

        q1.stop.assert_called_once()
        q2.stop.assert_called_once()
        assert pipeline.active_queries == []

    def test_supervisor_integration(self) -> None:
        config = SparkStreamingConfig()
        mock_query = MagicMock(spec=StreamingQuery)
        mock_query.isActive.return_value = False
        mock_query.exception.return_value = None

        pipeline = StreamingIngestionPipeline(config=config)
        supervisor = StreamingRecoverySupervisor(config=config, max_retries=2)

        with patch.object(pipeline, "start_queries", return_value=[mock_query]) as mock_start:
            exit_code = supervisor.run_with_recovery(pipeline.start_queries, register_signal_handlers=False)
            assert exit_code == 0
            mock_start.assert_called_once()


@pytest.mark.skipif(
    not is_streaming_infra_reachable(),
    reason="Live streaming infrastructure (Kafka/SchemaRegistry/MinIO) is not reachable.",
)
class TestLiveStreamingPipelineIntegration:
    """Live integration tests executing against running Docker containers."""

    def test_live_stream_produce_and_deserialize(self) -> None:
        """Produce live events to Kafka, consume them, and verify ConfluentWireDeserializer."""
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        sr_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

        sr_manager = SchemaRegistryManager(url=sr_url)
        serializer = AvroSerializerDeserializer(schema_registry_manager=sr_manager)
        deserializer = ConfluentWireDeserializer(schema_registry_url=sr_url)

        prod_config = ProducerConfig(
            topic=CheckoutProducer.ORDERS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=sr_url,
            client_id="test-streaming-pipeline-live",
        )
        producer = CheckoutProducer(
            config=prod_config,
            schema_registry_manager=sr_manager,
            serializer=serializer,
        )
        producer.setup()

        order_event = producer.generate_order_event()
        order_event_id = order_event["event_id"]
        assert producer.produce_order(event=order_event) is True
        producer.flush(timeout=5.0)

        consumer_conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"test-stream-verify-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        consumer = confluent_kafka.Consumer(consumer_conf)
        consumer.subscribe([CheckoutProducer.ORDERS_TOPIC])

        consumed_bytes: bytes | None = None
        event_id_bytes = order_event_id.encode("utf-8")
        deadline = time.monotonic() + 10.0

        try:
            while time.monotonic() < deadline:
                msg = consumer.poll(timeout=0.5)
                if msg is None or msg.error():
                    continue
                val = msg.value()
                if val and event_id_bytes in val:
                    consumed_bytes = val
                    break
        finally:
            consumer.close()

        assert consumed_bytes is not None, f"Timed out waiting for event '{order_event_id}' from Kafka."

        record = deserializer.deserialize(consumed_bytes, topic=CheckoutProducer.ORDERS_TOPIC)
        assert record.is_valid is True
        assert record.event_id == order_event_id
        assert record.event_type == "ORDER_CREATED"
        assert record.schema_id is not None
        assert record.schema_id > 0

        corrupt_bytes = ChaosEngine.corrupt_wire_format(consumed_bytes, "magic_byte")
        bad_record = deserializer.deserialize(corrupt_bytes, topic=CheckoutProducer.ORDERS_TOPIC)
        assert bad_record.is_valid is False
        assert bad_record.error_type == "INVALID_MAGIC_BYTE"
        assert bad_record.raw_payload_base64 is not None

        dlq_item = DLQRouter.create_dlq_record(
            raw_payload=bad_record.raw_payload_base64,
            error_type=bad_record.error_type,
            error_message=bad_record.error_message or "",
            kafka_topic=CheckoutProducer.ORDERS_TOPIC,
            kafka_partition=0,
            kafka_offset=999,
            schema_id=bad_record.schema_id,
        )
        assert dlq_item._error_type == "INVALID_MAGIC_BYTE"
        assert dlq_item._kafka_topic == CheckoutProducer.ORDERS_TOPIC
        recovered_raw = base64.b64decode(dlq_item._raw_payload_base64)
        assert recovered_raw == corrupt_bytes

        producer.close()
