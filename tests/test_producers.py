"""Unit tests for event producers framework and synthetic data generator."""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from ingestion.producers.antifraud_producer import AntiFraudProducer
from ingestion.producers.base_producer import BaseProducer, ProducerMetrics
from ingestion.producers.checkout_producer import CheckoutProducer
from ingestion.producers.config import (
    ProducerConfig,
    build_producer_arg_parser,
    parse_producer_config,
)
from ingestion.producers.generator import SyntheticDataGenerator
from ingestion.producers.inventory_producer import InventoryProducer
from ingestion.producers.navigation_producer import NavigationProducer
from ingestion.producers.payment_producer import PaymentProducer
from ingestion.schema_registry import SchemaRegistryManager
from ingestion.serialization import AvroSerializerDeserializer


class DummyProducer(BaseProducer):
    """Concrete BaseProducer subclass for unit testing."""

    def __init__(self, config: ProducerConfig, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)
        self.generator = SyntheticDataGenerator(seed=42)

    def generate_event(self) -> dict[str, Any]:
        return self.generator.generate_order_creation()


class TestProducerConfig:
    """Test suite for ProducerConfig and CLI argument parsing."""

    def test_default_config(self) -> None:
        config = ProducerConfig(topic="marketplace.orders")
        assert config.topic == "marketplace.orders"
        assert config.subject == "marketplace.orders-value"
        assert config.events_per_second == 5.0
        assert config.max_events is None
        assert config.acks == "all"

        kafka_dict = config.to_kafka_config()
        assert kafka_dict["bootstrap.servers"] == config.bootstrap_servers
        assert kafka_dict["client.id"] == "marketplace-producer"
        assert kafka_dict["acks"] == "all"

    def test_custom_subject_and_schema_path(self, tmp_path: Path) -> None:
        schema_file = tmp_path / "custom.avsc"
        schema_file.write_text("{}")
        config = ProducerConfig(
            topic="marketplace.custom",
            subject="custom-subject-value",
            schema_path=str(schema_file),
            events_per_second=20.0,
            max_events=100,
        )
        assert config.subject == "custom-subject-value"
        assert config.schema_path == schema_file
        assert config.events_per_second == 20.0
        assert config.max_events == 100

    def test_arg_parser_and_parse_config(self) -> None:
        parser = build_producer_arg_parser(default_topic="marketplace.payments", default_rate=10.0)
        cli_args = [
            "--topic",
            "marketplace.payments",
            "--rate",
            "15.5",
            "--max-events",
            "50",
            "--runtime-seconds",
            "10.0",
            "--client-id",
            "test-client",
            "--compression",
            "gzip",
        ]
        config = parse_producer_config(parser=parser, args=cli_args)
        assert config.topic == "marketplace.payments"
        assert config.subject == "marketplace.payments-value"
        assert config.events_per_second == 15.5
        assert config.max_events == 50
        assert config.runtime_seconds == 10.0
        assert config.client_id == "test-client"
        assert config.compression_type == "gzip"


class TestSyntheticDataGenerator:
    """Test suite for SyntheticDataGenerator and schema validity."""

    @pytest.fixture
    def serializer(self) -> AvroSerializerDeserializer:
        return AvroSerializerDeserializer()

    @pytest.fixture
    def generator(self) -> SyntheticDataGenerator:
        return SyntheticDataGenerator(seed=12345)

    def _load_schema(self, filename: str) -> dict[str, Any]:
        schema_path = Path(__file__).resolve().parent.parent / "ingestion" / "schemas" / "avro" / filename
        with schema_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def test_order_creation_generation_and_validation(
        self, generator: SyntheticDataGenerator, serializer: AvroSerializerDeserializer
    ) -> None:
        schema = self._load_schema("order_creation.avsc")
        for _ in range(5):
            event = generator.generate_order_creation()
            assert event["event_type"] == "ORDER_CREATED"
            assert isinstance(event["total_amount"], Decimal)
            assert len(event["items"]) > 0
            assert serializer.validate(event, schema) is True

    def test_payment_update_generation_and_validation(
        self, generator: SyntheticDataGenerator, serializer: AvroSerializerDeserializer
    ) -> None:
        schema = self._load_schema("payment_updates.avsc")
        for _ in range(5):
            event = generator.generate_payment_update()
            assert event["event_type"] == "PAYMENT_STATUS_UPDATED"
            assert isinstance(event["amount"], Decimal)
            assert serializer.validate(event, schema) is True

    def test_cart_event_generation_and_validation(
        self, generator: SyntheticDataGenerator, serializer: AvroSerializerDeserializer
    ) -> None:
        schema = self._load_schema("cart_events.avsc")
        for _ in range(5):
            event = generator.generate_cart_event()
            assert event["event_type"].startswith("CART_")
            assert isinstance(event["cart_total_value"], Decimal)
            assert serializer.validate(event, schema) is True

    def test_stock_update_generation_and_validation(
        self, generator: SyntheticDataGenerator, serializer: AvroSerializerDeserializer
    ) -> None:
        schema = self._load_schema("stock_updates.avsc")
        for _ in range(5):
            event = generator.generate_stock_update()
            assert event["event_type"].startswith("STOCK_")
            assert serializer.validate(event, schema) is True

    def test_catalog_change_generation_and_validation(
        self, generator: SyntheticDataGenerator, serializer: AvroSerializerDeserializer
    ) -> None:
        schema = self._load_schema("catalog_changes.avsc")
        for _ in range(5):
            event = generator.generate_catalog_change()
            assert event["event_type"] in ["PRODUCT_CREATED", "PRODUCT_UPDATED", "PRICE_CHANGED", "PRODUCT_DEACTIVATED"]
            assert isinstance(event["price"], Decimal)
            assert serializer.validate(event, schema) is True

    def test_user_navigation_generation_and_validation(
        self, generator: SyntheticDataGenerator, serializer: AvroSerializerDeserializer
    ) -> None:
        schema = self._load_schema("user_navigation.avsc")
        for _ in range(5):
            event = generator.generate_user_navigation()
            assert event["event_type"] in ["PAGE_VIEW", "PRODUCT_CLICK", "SEARCH_QUERY", "CATEGORY_BROWSE"]
            assert serializer.validate(event, schema) is True


class TestBaseProducer:
    """Test suite for BaseProducer abstraction, callbacks, and lifecycle."""

    def test_metrics_calculation(self) -> None:
        metrics = ProducerMetrics(produced_count=10, delivered_count=8, error_count=2, start_time=100.0, end_time=104.0)
        assert metrics.elapsed_seconds == 4.0
        assert metrics.effective_throughput == 2.0

    def test_producer_produce_one_and_delivery_callback(self) -> None:
        mock_kafka_producer = MagicMock()
        mock_schema_registry = MagicMock(spec=SchemaRegistryManager)
        mock_schema_registry.register_schema.return_value = 42

        config = ProducerConfig(topic="marketplace.orders")
        producer = DummyProducer(
            config=config,
            schema_registry_manager=mock_schema_registry,
            producer=mock_kafka_producer,
        )

        producer.setup()
        assert producer.schema_id == 42
        assert mock_schema_registry.register_schema.called

        # Produce one event
        success = producer.produce_one()
        assert success is True
        assert producer.metrics.produced_count == 1
        assert mock_kafka_producer.produce.called
        assert mock_kafka_producer.poll.called

        # Test delivery callback success
        mock_msg = MagicMock()
        mock_msg.topic.return_value = "marketplace.orders"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 1
        producer._delivery_callback(None, mock_msg)
        assert producer.metrics.delivered_count == 1

        # Test delivery callback error
        producer._delivery_callback("Broker transport failure", mock_msg)
        assert producer.metrics.error_count == 1

    def test_producer_run_with_max_events(self) -> None:
        mock_kafka_producer = MagicMock()
        mock_schema_registry = MagicMock(spec=SchemaRegistryManager)
        mock_schema_registry.register_schema.return_value = 1

        config = ProducerConfig(topic="marketplace.orders", events_per_second=100.0, max_events=5)
        producer = DummyProducer(
            config=config,
            schema_registry_manager=mock_schema_registry,
            producer=mock_kafka_producer,
        )

        producer.run()
        assert producer.metrics.produced_count == 5
        assert mock_kafka_producer.flush.called


class TestDomainProducers:
    """Test suite for domain-specific event producers."""

    @pytest.fixture
    def mock_kafka(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def mock_sr(self) -> MagicMock:
        sr = MagicMock(spec=SchemaRegistryManager)
        sr.register_schema.return_value = 101
        return sr

    def test_checkout_producer_modes_and_routing(self, mock_kafka: MagicMock, mock_sr: MagicMock) -> None:
        producer = CheckoutProducer(
            mode="blended",
            producer=mock_kafka,
            schema_registry_manager=mock_sr,
        )
        producer.setup()

        # Order creation
        order_success = producer.produce_order()
        assert order_success is True

        # Cart event
        cart_success = producer.produce_cart()
        assert cart_success is True

        # Auto-routing based on payload
        cart_event = producer.generate_cart_event()
        producer.produce_one(cart_event)
        last_topic = mock_kafka.produce.call_args.kwargs["topic"]
        assert last_topic == CheckoutProducer.CART_TOPIC

    def test_payment_producer_lifecycle(self, mock_kafka: MagicMock, mock_sr: MagicMock) -> None:
        producer = PaymentProducer(producer=mock_kafka, schema_registry_manager=mock_sr)
        producer.setup()

        lifecycle_events = producer.simulate_payment_lifecycle(
            order_id="00000000-0000-0000-0000-000000000001",
            customer_id="00000000-0000-0000-0000-000000000002",
            amount=Decimal("150.0000"),
        )
        assert len(lifecycle_events) == 2
        assert lifecycle_events[0]["payment_status"] == "AUTHORIZED"
        assert lifecycle_events[1]["payment_status"] in ("APPROVED", "DECLINED", "FAILED")
        assert lifecycle_events[0]["payment_id"] == lifecycle_events[1]["payment_id"]

        results = producer.produce_payment_lifecycle(
            order_id="00000000-0000-0000-0000-000000000001",
            customer_id="00000000-0000-0000-0000-000000000002",
            amount=Decimal("150.0000"),
        )
        assert all(results)

    def test_navigation_producer_journey(self, mock_kafka: MagicMock, mock_sr: MagicMock) -> None:
        producer = NavigationProducer(producer=mock_kafka, schema_registry_manager=mock_sr)
        producer.setup()

        journey = producer.simulate_user_journey()
        assert len(journey) == 4
        assert [e["event_type"] for e in journey] == ["PAGE_VIEW", "SEARCH_QUERY", "CATEGORY_BROWSE", "PRODUCT_CLICK"]
        assert all(e["session_id"] == journey[0]["session_id"] for e in journey)

        results = producer.produce_user_journey()
        assert all(results)

    def test_inventory_producer_modes_and_routing(self, mock_kafka: MagicMock, mock_sr: MagicMock) -> None:
        producer = InventoryProducer(
            mode="blended",
            producer=mock_kafka,
            schema_registry_manager=mock_sr,
        )
        producer.setup()

        stock_success = producer.produce_stock_update()
        assert stock_success is True

        catalog_success = producer.produce_catalog_change()
        assert catalog_success is True

        # Auto-routing based on payload
        cat_event = producer.generate_catalog_event()
        producer.produce_one(cat_event)
        last_topic = mock_kafka.produce.call_args.kwargs["topic"]
        assert last_topic == InventoryProducer.CATALOG_TOPIC


class TestAntiFraudProducer:
    """Test suite for AntiFraudProducer, anomaly generation, Avro validation, and Kafka production."""

    @pytest.fixture
    def serializer(self) -> AvroSerializerDeserializer:
        return AvroSerializerDeserializer()

    @pytest.fixture
    def generator(self) -> SyntheticDataGenerator:
        return SyntheticDataGenerator(seed=42)

    @pytest.fixture
    def order_schema(self) -> dict[str, Any]:
        schema_path = Path(__file__).resolve().parent.parent / "ingestion" / "schemas" / "avro" / "order_creation.avsc"
        with schema_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @pytest.fixture
    def payment_schema(self) -> dict[str, Any]:
        schema_path = Path(__file__).resolve().parent.parent / "ingestion" / "schemas" / "avro" / "payment_updates.avsc"
        with schema_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @pytest.fixture
    def mock_kafka(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def mock_sr(self) -> MagicMock:
        sr = MagicMock(spec=SchemaRegistryManager)
        sr.register_schema.return_value = 201
        return sr

    def test_init_invalid_pattern_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid pattern 'fraud_bot'"):
            AntiFraudProducer(pattern="fraud_bot")

    def test_generate_high_value_order_schema_and_range(
        self,
        generator: SyntheticDataGenerator,
        serializer: AvroSerializerDeserializer,
        order_schema: dict[str, Any],
    ) -> None:
        producer = AntiFraudProducer(generator=generator)
        order = producer.generate_high_value_order(multiplier=10.0)

        assert order["event_type"] == "ORDER_CREATED"
        assert isinstance(order["total_amount"], Decimal)
        assert isinstance(order["subtotal_amount"], Decimal)
        # Verify unusual high value threshold: Decimal("45000.0000") to Decimal("99000.0000")
        assert order["total_amount"] >= Decimal("45000.0000")
        assert order["total_amount"] <= Decimal("99000.0000")

        # Verify math integrity
        subtotal_sum = sum(item["subtotal"] for item in order["items"])
        assert order["subtotal_amount"] == subtotal_sum
        assert order["total_amount"] == subtotal_sum + order["shipping_fee"] - order["discount_amount"]

        # Validate against Avro schema
        assert serializer.validate(order, order_schema) is True

    def test_generate_high_value_order_invalid_multiplier(self) -> None:
        producer = AntiFraudProducer()
        with pytest.raises(ValueError, match="Multiplier must be positive"):
            producer.generate_high_value_order(multiplier=0.0)
        with pytest.raises(ValueError, match="Multiplier must be positive"):
            producer.generate_high_value_order(multiplier=-5.0)

    def test_generate_velocity_burst(
        self,
        generator: SyntheticDataGenerator,
        serializer: AvroSerializerDeserializer,
        order_schema: dict[str, Any],
    ) -> None:
        producer = AntiFraudProducer(generator=generator)
        burst_count = 5
        orders = producer.generate_velocity_burst(count=burst_count)

        assert len(orders) == burst_count
        first_cust = orders[0]["customer_id"]
        assert all(o["customer_id"] == first_cust for o in orders)

        # Unique order and event IDs
        order_ids = {o["order_id"] for o in orders}
        assert len(order_ids) == burst_count
        event_ids = {o["event_id"] for o in orders}
        assert len(event_ids) == burst_count

        # Timestamps should be ordered and closely spaced (within a few seconds)
        for i in range(len(orders) - 1):
            assert orders[i]["timestamp"] <= orders[i + 1]["timestamp"]
        total_time_diff = (orders[-1]["timestamp"] - orders[0]["timestamp"]).total_seconds()
        assert total_time_diff < 5.0

        # Validate each order against schema
        for order in orders:
            assert serializer.validate(order, order_schema) is True

    def test_generate_velocity_burst_custom_customer_and_validation(self) -> None:
        producer = AntiFraudProducer()
        custom_cust = "00000000-1111-2222-3333-444444444444"
        orders = producer.generate_velocity_burst(customer_id=custom_cust, count=3)
        assert len(orders) == 3
        assert all(o["customer_id"] == custom_cust for o in orders)

        with pytest.raises(ValueError, match="Count must be at least 1"):
            producer.generate_velocity_burst(count=0)

    def test_generate_geo_anomaly(
        self,
        generator: SyntheticDataGenerator,
        serializer: AvroSerializerDeserializer,
        order_schema: dict[str, Any],
    ) -> None:
        producer = AntiFraudProducer(generator=generator)
        orders = producer.generate_geo_anomaly()

        assert len(orders) == 2
        # Same customer placed both orders
        assert orders[0]["customer_id"] == orders[1]["customer_id"]
        # Different orders
        assert orders[0]["order_id"] != orders[1]["order_id"]

        # Different states (São Paulo vs Amazonas)
        assert orders[0]["shipping_address"]["state"] == "SP"
        assert orders[1]["shipping_address"]["state"] == "AM"

        # Impossible rapid travel interval (<= 5 seconds)
        delta_sec = (orders[1]["timestamp"] - orders[0]["timestamp"]).total_seconds()
        assert 0.0 < delta_sec <= 5.0

        # Validate both against schema
        assert serializer.validate(orders[0], order_schema) is True
        assert serializer.validate(orders[1], order_schema) is True

    def test_generate_card_testing_pattern(
        self,
        generator: SyntheticDataGenerator,
        serializer: AvroSerializerDeserializer,
        payment_schema: dict[str, Any],
    ) -> None:
        producer = AntiFraudProducer(generator=generator)
        attempts = 4
        payment_events = producer.generate_card_testing_pattern(attempts=attempts)

        assert len(payment_events) == attempts
        target_order_id = payment_events[0]["order_id"]
        target_customer_id = payment_events[0]["customer_id"]

        # All events belong to the same checkout order and customer
        assert all(p["order_id"] == target_order_id for p in payment_events)
        assert all(p["customer_id"] == target_customer_id for p in payment_events)

        # Unique payment transactions
        payment_ids = {p["payment_id"] for p in payment_events}
        assert len(payment_ids) == attempts
        txn_refs = {p["transaction_reference"] for p in payment_events}
        assert len(txn_refs) == attempts

        # All attempts must be terminal declines or gateway failures
        for p in payment_events:
            assert p["event_type"] == "PAYMENT_STATUS_UPDATED"
            assert p["payment_status"] in ("DECLINED", "FAILED")
            assert p["failure_reason"] is not None
            assert serializer.validate(p, payment_schema) is True

    def test_generate_card_testing_invalid_attempts(self) -> None:
        producer = AntiFraudProducer()
        with pytest.raises(ValueError, match="Attempts must be at least 1"):
            producer.generate_card_testing_pattern(attempts=0)

    def test_generate_event_conforms_to_order_schema(
        self,
        serializer: AvroSerializerDeserializer,
        order_schema: dict[str, Any],
    ) -> None:
        for pat in ["high_value", "velocity", "geo"]:
            producer = AntiFraudProducer(pattern=pat)
            event = producer.generate_event()
            assert event["event_type"] == "ORDER_CREATED"
            assert serializer.validate(event, order_schema) is True

    def test_produce_anomaly_dispatch(
        self,
        mock_kafka: MagicMock,
        mock_sr: MagicMock,
    ) -> None:
        producer = AntiFraudProducer(producer=mock_kafka, schema_registry_manager=mock_sr)
        producer.setup()

        # High value produces single order
        hv_result = producer.produce_anomaly("high_value")
        assert hv_result is True
        assert mock_kafka.produce.called
        assert mock_kafka.produce.call_args.kwargs["topic"] == AntiFraudProducer.ORDERS_TOPIC

        # Velocity burst produces multiple orders
        mock_kafka.produce.reset_mock()
        vel_results = producer.produce_anomaly("velocity")
        assert isinstance(vel_results, list)
        assert len(vel_results) == 5
        assert all(vel_results)
        assert mock_kafka.produce.call_count == 5
        assert mock_kafka.produce.call_args.kwargs["topic"] == AntiFraudProducer.ORDERS_TOPIC

        # Geo anomaly produces 2 orders
        mock_kafka.produce.reset_mock()
        geo_results = producer.produce_anomaly("geo")
        assert isinstance(geo_results, list)
        assert len(geo_results) == 2
        assert all(geo_results)
        assert mock_kafka.produce.call_count == 2
        assert mock_kafka.produce.call_args.kwargs["topic"] == AntiFraudProducer.ORDERS_TOPIC

        # Card testing produces payment events to marketplace.payments
        mock_kafka.produce.reset_mock()
        card_results = producer.produce_anomaly("card_testing")
        assert isinstance(card_results, list)
        assert len(card_results) == 3
        assert all(card_results)
        assert mock_kafka.produce.call_count == 3
        assert mock_kafka.produce.call_args.kwargs["topic"] == AntiFraudProducer.PAYMENTS_TOPIC

        # Invalid pattern raises
        with pytest.raises(ValueError, match="Invalid pattern 'unknown_pattern'"):
            producer.produce_anomaly("unknown_pattern")
