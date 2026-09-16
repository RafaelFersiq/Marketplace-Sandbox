"""Unit tests for event producers framework and synthetic data generator."""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from ingestion.producers.base_producer import BaseProducer, ProducerMetrics
from ingestion.producers.config import (
    ProducerConfig,
    build_producer_arg_parser,
    parse_producer_config,
)
from ingestion.producers.generator import SyntheticDataGenerator
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
