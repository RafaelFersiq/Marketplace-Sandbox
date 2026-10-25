"""Integration test suite for live Kafka streaming and simulation runner verification.

Validates:
- Reachability checks for local Kafka broker and Confluent Schema Registry.
- End-to-end event publishing and consumption across domain producers:
    * CheckoutProducer (marketplace.orders, marketplace.cart)
    * PaymentProducer (marketplace.payments)
    * NavigationProducer (marketplace.navigation)
    * InventoryProducer (marketplace.inventory, marketplace.catalog)
    * AntiFraudProducer (marketplace.orders)
- Binary Confluent wire format parsing (magic byte 0x00 and 4-byte schema ID).
- Deserialization and Avro schema validation on consumed messages.
- Offline unit tests verifying simulation argument parsing, producer factory, and runner lifecycle.
"""

from __future__ import annotations

import os
import socket
import struct
import time
import uuid
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import urlparse

import confluent_kafka
import pytest
import requests

from ingestion.producers.antifraud_producer import AntiFraudProducer
from ingestion.producers.base_producer import BaseProducer
from ingestion.producers.chaos import ChaosConfig, ChaosEngine
from ingestion.producers.checkout_producer import CheckoutProducer
from ingestion.producers.config import ProducerConfig
from ingestion.producers.inventory_producer import InventoryProducer
from ingestion.producers.navigation_producer import NavigationProducer
from ingestion.producers.payment_producer import PaymentProducer
from ingestion.schema_registry import SchemaRegistryManager
from ingestion.serialization import AvroSerializerDeserializer
from scripts.run_producer_simulation import (
    SimulationRunner,
    build_simulation_arg_parser,
    create_producer,
    normalize_producer_names,
)


def is_kafka_reachable(bootstrap_servers: str = "localhost:9092", timeout: float = 1.0) -> bool:
    """Check if Kafka broker TCP port is reachable.

    Args:
        bootstrap_servers: Comma-separated list of host:port broker endpoints.
        timeout: Socket connection timeout in seconds.

    Returns:
        True if connection succeeded, False otherwise.
    """
    try:
        first_broker = bootstrap_servers.split(",")[0].strip()
        host, port_str = first_broker.split(":")
        with socket.create_connection((host, int(port_str)), timeout=timeout):
            return True
    except (TimeoutError, OSError, ValueError):
        return False


def is_schema_registry_reachable(url: str = "http://localhost:8081", timeout: float = 1.0) -> bool:
    """Check if Confluent Schema Registry service is reachable.

    Args:
        url: Schema Registry base URL.
        timeout: HTTP request / socket timeout in seconds.

    Returns:
        True if service responds with HTTP 200, False otherwise.
    """
    try:
        parsed = urlparse(url)
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        with socket.create_connection((host, port), timeout=timeout):
            resp = requests.get(f"{url.rstrip('/')}/subjects", timeout=timeout)
            return resp.status_code == 200
    except (requests.RequestException, OSError, ValueError):
        return False


@pytest.fixture(scope="module")
def live_services_check() -> dict[str, str]:
    """Pytest fixture checking if local Kafka and Schema Registry are online.

    Skips live integration tests gracefully if services are unavailable.

    Returns:
        Dictionary containing configured bootstrap_servers and schema_registry_url.
    """
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    sr_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

    if not is_kafka_reachable(kafka_servers):
        pytest.skip(f"Kafka broker ({kafka_servers}) is offline. Skipping live integration tests.")
    if not is_schema_registry_reachable(sr_url):
        pytest.skip(f"Confluent Schema Registry ({sr_url}) is offline. Skipping live integration tests.")

    return {"bootstrap_servers": kafka_servers, "schema_registry_url": sr_url}


def consume_matching_event(
    bootstrap_servers: str,
    topic: str,
    event_id: str,
    timeout: float = 10.0,
) -> tuple[bytes, confluent_kafka.Message]:
    """Consume a message from Kafka matching the given event_id.

    Args:
        bootstrap_servers: Kafka broker addresses.
        topic: Kafka topic to consume from.
        event_id: Event ID string to match.
        timeout: Maximum seconds to wait.

    Returns:
        Tuple of (message_value_bytes, message_object).

    Raises:
        TimeoutError: If matching message is not received within timeout.
    """
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"test-verify-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = confluent_kafka.Consumer(conf)
    consumer.subscribe([topic])

    start_time = time.monotonic()
    event_id_bytes = event_id.encode("utf-8")

    try:
        while time.monotonic() - start_time < timeout:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                continue

            val = msg.value()
            if val is not None and event_id_bytes in val:
                return val, msg
        raise TimeoutError(f"Timed out waiting for event_id '{event_id}' on topic '{topic}' after {timeout}s")
    finally:
        consumer.close()


# ==============================================================================
# Live End-to-End Producer Tests
# ==============================================================================


class TestLiveProducersIntegration:
    """Live streaming verification against running Kafka and Schema Registry containers."""

    def test_live_checkout_producer_orders_and_cart(self, live_services_check: dict[str, str]) -> None:
        """Test producing live OrderCreationEvent and CartEvent, consuming and validating them."""
        bootstrap_servers = live_services_check["bootstrap_servers"]
        sr_url = live_services_check["schema_registry_url"]

        sr_manager = SchemaRegistryManager(url=sr_url)
        serializer = AvroSerializerDeserializer(schema_registry_manager=sr_manager)

        config = ProducerConfig(
            topic=CheckoutProducer.ORDERS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=sr_url,
            client_id="test-live-checkout",
        )
        producer = CheckoutProducer(
            config=config,
            mode="blended",
            schema_registry_manager=sr_manager,
            serializer=serializer,
        )
        producer.setup()

        # 1. Produce OrderCreationEvent
        order_event = producer.generate_order_event()
        order_id = order_event["order_id"]
        order_event_id = order_event["event_id"]
        assert producer.produce_order(event=order_event) is True
        producer.flush(timeout=5.0)

        # Consume and verify
        raw_order_bytes, _ = consume_matching_event(
            bootstrap_servers=bootstrap_servers,
            topic=CheckoutProducer.ORDERS_TOPIC,
            event_id=order_event_id,
        )
        assert raw_order_bytes[0:1] == b"\x00"
        order_schema_id = struct.unpack(">I", raw_order_bytes[1:5])[0]
        assert order_schema_id > 0

        decoded_order_id, decoded_order = serializer.deserialize(raw_order_bytes)
        assert decoded_order_id == order_schema_id
        assert decoded_order["event_id"] == order_event_id
        assert decoded_order["order_id"] == order_id

        # 2. Produce CartEvent
        cart_event = producer.generate_cart_event()
        cart_id = cart_event["cart_id"]
        cart_event_id = cart_event["event_id"]
        assert producer.produce_cart(event=cart_event) is True
        producer.flush(timeout=5.0)

        # Consume and verify
        raw_cart_bytes, _ = consume_matching_event(
            bootstrap_servers=bootstrap_servers,
            topic=CheckoutProducer.CART_TOPIC,
            event_id=cart_event_id,
        )
        assert raw_cart_bytes[0:1] == b"\x00"
        cart_schema_id = struct.unpack(">I", raw_cart_bytes[1:5])[0]
        assert cart_schema_id > 0

        decoded_cart_id, decoded_cart = serializer.deserialize(raw_cart_bytes)
        assert decoded_cart_id == cart_schema_id
        assert decoded_cart["event_id"] == cart_event_id
        assert decoded_cart["cart_id"] == cart_id

        producer.close()

    def test_live_payment_producer(self, live_services_check: dict[str, str]) -> None:
        """Test producing live PaymentUpdateEvent, consuming and deserializing it."""
        bootstrap_servers = live_services_check["bootstrap_servers"]
        sr_url = live_services_check["schema_registry_url"]

        sr_manager = SchemaRegistryManager(url=sr_url)
        serializer = AvroSerializerDeserializer(schema_registry_manager=sr_manager)

        config = ProducerConfig(
            topic=PaymentProducer.PAYMENTS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=sr_url,
            client_id="test-live-payment",
        )
        producer = PaymentProducer(
            config=config,
            schema_registry_manager=sr_manager,
            serializer=serializer,
        )
        producer.setup()

        event = producer.generate_event()
        event_id = event["event_id"]
        payment_id = event["payment_id"]

        assert producer.produce_one(event=event) is True
        producer.flush(timeout=5.0)

        raw_bytes, _ = consume_matching_event(
            bootstrap_servers=bootstrap_servers,
            topic=PaymentProducer.PAYMENTS_TOPIC,
            event_id=event_id,
        )
        assert raw_bytes[0:1] == b"\x00"
        schema_id = struct.unpack(">I", raw_bytes[1:5])[0]
        assert schema_id > 0

        decoded_id, decoded_event = serializer.deserialize(raw_bytes)
        assert decoded_id == schema_id
        assert decoded_event["event_id"] == event_id
        assert decoded_event["payment_id"] == payment_id
        assert decoded_event["payment_status"] in {"AUTHORIZED", "APPROVED", "DECLINED", "REFUNDED", "FAILED"}

        producer.close()

    def test_live_navigation_producer(self, live_services_check: dict[str, str]) -> None:
        """Test producing live UserNavigationEvent, consuming and deserializing it."""
        bootstrap_servers = live_services_check["bootstrap_servers"]
        sr_url = live_services_check["schema_registry_url"]

        sr_manager = SchemaRegistryManager(url=sr_url)
        serializer = AvroSerializerDeserializer(schema_registry_manager=sr_manager)

        config = ProducerConfig(
            topic=NavigationProducer.NAVIGATION_TOPIC,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=sr_url,
            client_id="test-live-nav",
        )
        producer = NavigationProducer(
            config=config,
            schema_registry_manager=sr_manager,
            serializer=serializer,
        )
        producer.setup()

        event = producer.generate_event()
        event_id = event["event_id"]
        session_id = event["session_id"]

        assert producer.produce_one(event=event) is True
        producer.flush(timeout=5.0)

        raw_bytes, _ = consume_matching_event(
            bootstrap_servers=bootstrap_servers,
            topic=NavigationProducer.NAVIGATION_TOPIC,
            event_id=event_id,
        )
        assert raw_bytes[0:1] == b"\x00"
        schema_id = struct.unpack(">I", raw_bytes[1:5])[0]
        assert schema_id > 0

        decoded_id, decoded_event = serializer.deserialize(raw_bytes)
        assert decoded_id == schema_id
        assert decoded_event["event_id"] == event_id
        assert decoded_event["session_id"] == session_id

        producer.close()

    def test_live_inventory_producer_stock_and_catalog(self, live_services_check: dict[str, str]) -> None:
        """Test producing live StockUpdateEvent and CatalogChangeEvent, consuming and deserializing them."""
        bootstrap_servers = live_services_check["bootstrap_servers"]
        sr_url = live_services_check["schema_registry_url"]

        sr_manager = SchemaRegistryManager(url=sr_url)
        serializer = AvroSerializerDeserializer(schema_registry_manager=sr_manager)

        config = ProducerConfig(
            topic=InventoryProducer.INVENTORY_TOPIC,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=sr_url,
            client_id="test-live-inventory",
        )
        producer = InventoryProducer(
            config=config,
            mode="blended",
            schema_registry_manager=sr_manager,
            serializer=serializer,
        )
        producer.setup()

        # 1. Stock update
        stock_event = producer.generate_stock_event()
        stock_event_id = stock_event["event_id"]
        product_id = stock_event["product_id"]

        assert producer.produce_stock_update(event=stock_event) is True
        producer.flush(timeout=5.0)

        raw_stock_bytes, _ = consume_matching_event(
            bootstrap_servers=bootstrap_servers,
            topic=InventoryProducer.INVENTORY_TOPIC,
            event_id=stock_event_id,
        )
        assert raw_stock_bytes[0:1] == b"\x00"
        stock_schema_id = struct.unpack(">I", raw_stock_bytes[1:5])[0]
        assert stock_schema_id > 0

        decoded_stock_id, decoded_stock = serializer.deserialize(raw_stock_bytes)
        assert decoded_stock_id == stock_schema_id
        assert decoded_stock["event_id"] == stock_event_id
        assert decoded_stock["product_id"] == product_id

        # 2. Catalog change
        catalog_event = producer.generate_catalog_event()
        catalog_event_id = catalog_event["event_id"]

        assert producer.produce_catalog_change(event=catalog_event) is True
        producer.flush(timeout=5.0)

        raw_cat_bytes, _ = consume_matching_event(
            bootstrap_servers=bootstrap_servers,
            topic=InventoryProducer.CATALOG_TOPIC,
            event_id=catalog_event_id,
        )
        assert raw_cat_bytes[0:1] == b"\x00"
        cat_schema_id = struct.unpack(">I", raw_cat_bytes[1:5])[0]
        assert cat_schema_id > 0

        decoded_cat_id, decoded_cat = serializer.deserialize(raw_cat_bytes)
        assert decoded_cat_id == cat_schema_id
        assert decoded_cat["event_id"] == catalog_event_id

        producer.close()

    def test_live_antifraud_producer(self, live_services_check: dict[str, str]) -> None:
        """Test producing live suspicious fraud telemetry to Kafka, consuming and deserializing it."""
        bootstrap_servers = live_services_check["bootstrap_servers"]
        sr_url = live_services_check["schema_registry_url"]

        sr_manager = SchemaRegistryManager(url=sr_url)
        serializer = AvroSerializerDeserializer(schema_registry_manager=sr_manager)

        config = ProducerConfig(
            topic=AntiFraudProducer.ORDERS_TOPIC,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=sr_url,
            client_id="test-live-antifraud",
        )
        producer = AntiFraudProducer(
            config=config,
            pattern="high_value",
            schema_registry_manager=sr_manager,
            serializer=serializer,
        )
        producer.setup()

        event = producer.generate_event()
        event_id = event["event_id"]

        assert producer.produce_one(event=event) is True
        producer.flush(timeout=5.0)

        raw_bytes, _ = consume_matching_event(
            bootstrap_servers=bootstrap_servers,
            topic=AntiFraudProducer.ORDERS_TOPIC,
            event_id=event_id,
        )
        assert raw_bytes[0:1] == b"\x00"
        schema_id = struct.unpack(">I", raw_bytes[1:5])[0]
        assert schema_id > 0

        decoded_id, decoded_event = serializer.deserialize(raw_bytes)
        assert decoded_id == schema_id
        assert decoded_event["event_id"] == event_id

        producer.close()


# ==============================================================================
# Offline Unit Tests for Simulation CLI and Runner
# ==============================================================================


class DummyTestProducer(BaseProducer):
    """Test BaseProducer implementation for offline unit tests."""

    def __init__(self, config: ProducerConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self.call_count = 0

    def generate_event(self) -> dict[str, Any]:
        self.call_count += 1
        return {"event_id": f"dummy-{self.call_count}", "data": "test"}

    def produce_one(self, event: dict[str, Any] | None = None, topic: str | None = None) -> bool:
        del event, topic
        self.metrics.produced_count += 1
        self.metrics.delivered_count += 1
        return True


class TestSimulationCliParsing:
    """Test suite for CLI argument parser and normalization utilities."""

    def test_default_cli_arguments(self) -> None:
        parser = build_simulation_arg_parser()
        args = parser.parse_args([])

        assert args.producers == ["all"]
        assert args.bootstrap_servers == "localhost:9092"
        assert args.schema_registry_url == "http://localhost:8081"
        assert args.rate is None
        assert args.runtime_seconds is None
        assert args.max_events is None
        assert args.enable_chaos is False
        assert args.chaos_duplicate_rate == 0.0
        assert args.chaos_delay_rate == 0.0
        assert args.chaos_invalid_rate == 0.0
        assert args.chaos_burst_prob == 0.0

    def test_custom_cli_arguments(self) -> None:
        parser = build_simulation_arg_parser()
        args = parser.parse_args(
            [
                "--producers",
                "checkout,payment",
                "--rate",
                "12.5",
                "--runtime-seconds",
                "30.0",
                "--max-events",
                "100",
                "--enable-chaos",
                "--chaos-duplicate-rate",
                "0.15",
                "--chaos-delay-rate",
                "0.10",
                "--chaos-invalid-rate",
                "0.05",
                "--chaos-burst-prob",
                "0.20",
            ]
        )

        assert args.producers == ["checkout,payment"]
        assert args.rate == 12.5
        assert args.runtime_seconds == 30.0
        assert args.max_events == 100
        assert args.enable_chaos is True
        assert args.chaos_duplicate_rate == 0.15
        assert args.chaos_delay_rate == 0.10
        assert args.chaos_invalid_rate == 0.05
        assert args.chaos_burst_prob == 0.20

    def test_normalize_producer_names(self) -> None:
        assert normalize_producer_names(["all"]) == [
            "checkout",
            "payment",
            "navigation",
            "inventory",
            "antifraud",
        ]
        assert normalize_producer_names(["checkout,payment"]) == ["checkout", "payment"]
        assert normalize_producer_names(["checkout", "payment", "checkout"]) == ["checkout", "payment"]
        assert normalize_producer_names(["navigation", "inventory,antifraud"]) == [
            "navigation",
            "inventory",
            "antifraud",
        ]

    def test_normalize_producer_names_invalid_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown producer 'unsupported_producer'"):
            normalize_producer_names(["unsupported_producer"])


class TestProducerFactory:
    """Test suite for create_producer factory routine."""

    def test_create_producer_checkout(self) -> None:
        sr_mock = MagicMock(spec=SchemaRegistryManager)
        ser_mock = MagicMock(spec=AvroSerializerDeserializer)
        producer = create_producer(
            "checkout",
            events_per_second=8.0,
            schema_registry_manager=sr_mock,
            serializer=ser_mock,
        )
        assert isinstance(producer, CheckoutProducer)
        assert producer.config.events_per_second == 8.0
        assert producer.mode == "blended"

    def test_create_producer_payment(self) -> None:
        sr_mock = MagicMock(spec=SchemaRegistryManager)
        ser_mock = MagicMock(spec=AvroSerializerDeserializer)
        producer = create_producer(
            "payment",
            events_per_second=6.0,
            schema_registry_manager=sr_mock,
            serializer=ser_mock,
        )
        assert isinstance(producer, PaymentProducer)
        assert producer.config.events_per_second == 6.0

    def test_create_producer_navigation_defaults(self) -> None:
        sr_mock = MagicMock(spec=SchemaRegistryManager)
        ser_mock = MagicMock(spec=AvroSerializerDeserializer)
        producer = create_producer(
            "navigation",
            schema_registry_manager=sr_mock,
            serializer=ser_mock,
        )
        assert isinstance(producer, NavigationProducer)
        assert producer.config.events_per_second == 20.0

    def test_create_producer_inventory(self) -> None:
        sr_mock = MagicMock(spec=SchemaRegistryManager)
        ser_mock = MagicMock(spec=AvroSerializerDeserializer)
        producer = create_producer(
            "inventory",
            events_per_second=7.0,
            schema_registry_manager=sr_mock,
            serializer=ser_mock,
        )
        assert isinstance(producer, InventoryProducer)
        assert producer.config.events_per_second == 7.0
        assert producer.mode == "blended"

    def test_create_producer_antifraud(self) -> None:
        sr_mock = MagicMock(spec=SchemaRegistryManager)
        ser_mock = MagicMock(spec=AvroSerializerDeserializer)
        producer = create_producer(
            "antifraud",
            events_per_second=3.0,
            schema_registry_manager=sr_mock,
            serializer=ser_mock,
        )
        assert isinstance(producer, AntiFraudProducer)
        assert producer.config.events_per_second == 3.0

    def test_create_producer_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown producer 'non_existent'"):
            create_producer("non_existent")


class TestSimulationRunnerMocked:
    """Test suite for SimulationRunner thread lifecycle and metrics calculation."""

    def test_runner_execution_with_max_events(self) -> None:
        config1 = ProducerConfig(topic="marketplace.orders", events_per_second=100.0, max_events=5)
        config2 = ProducerConfig(topic="marketplace.payments", events_per_second=100.0, max_events=5)

        p1 = DummyTestProducer(config=config1)
        p2 = DummyTestProducer(config=config2)

        runner = SimulationRunner(producers=[p1, p2])
        summary = runner.run()

        assert summary.total_produced == 10
        assert summary.total_delivered == 10
        assert summary.total_errors == 0
        assert summary.elapsed_seconds >= 0.0
        assert summary.aggregate_throughput >= 0.0
        assert "DummyTestProducer" in summary.producer_summaries
        assert summary.chaos_metrics is None

    def test_runner_execution_with_chaos(self) -> None:
        config = ProducerConfig(topic="marketplace.orders", events_per_second=100.0, max_events=5)
        p = DummyTestProducer(config=config)
        chaos_engine = ChaosEngine(config=ChaosConfig(delay_rate=1.0, seed=42))

        runner = SimulationRunner(producers=[p], chaos_engine=chaos_engine)
        summary = runner.run()

        assert summary.total_produced >= 5
        assert summary.chaos_metrics is not None
        assert summary.chaos_metrics["evaluated"] >= 5
        assert summary.chaos_metrics["delayed"] >= 5

    def test_runner_stop_signal(self) -> None:
        config = ProducerConfig(topic="marketplace.orders", events_per_second=10.0)
        p = DummyTestProducer(config=config)
        runner = SimulationRunner(producers=[p])

        # Stop immediately
        runner.stop()
        summary = runner.run(runtime_seconds=0.1)
        assert summary.total_errors == 0
