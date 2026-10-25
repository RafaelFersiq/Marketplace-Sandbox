"""Inventory and Catalog management Kafka event producer.

Publishes StockUpdateEvent to 'marketplace.inventory' and CatalogChangeEvent to 'marketplace.catalog'
topics with Confluent Schema Registry Avro validation and rate-limited streaming.
"""

import logging
import random
from typing import Any

import confluent_kafka

from ingestion.producers.base_producer import BaseProducer
from ingestion.producers.config import (
    ProducerConfig,
    build_producer_arg_parser,
    parse_producer_config,
)
from ingestion.producers.generator import SyntheticDataGenerator
from ingestion.schema_registry import SchemaRegistryManager
from ingestion.serialization import AvroSerializerDeserializer

logger = logging.getLogger("producers.inventory")


class InventoryProducer(BaseProducer):
    """Kafka event producer for warehouse inventory stock updates and merchant catalog changes."""

    INVENTORY_TOPIC: str = "marketplace.inventory"
    CATALOG_TOPIC: str = "marketplace.catalog"

    def __init__(
        self,
        config: ProducerConfig | None = None,
        mode: str = "stock",
        generator: SyntheticDataGenerator | None = None,
        schema_registry_manager: SchemaRegistryManager | None = None,
        serializer: AvroSerializerDeserializer | None = None,
        producer: confluent_kafka.Producer | None = None,
    ) -> None:
        """Initialize InventoryProducer.

        Args:
            config: Optional ProducerConfig. Defaults to INVENTORY_TOPIC config.
            mode: Event production mode: 'stock', 'catalog', or 'blended'.
            generator: Optional SyntheticDataGenerator instance.
            schema_registry_manager: Optional SchemaRegistryManager instance.
            serializer: Optional AvroSerializerDeserializer instance.
            producer: Optional pre-configured confluent_kafka.Producer.

        Raises:
            ValueError: If mode is not one of 'stock', 'catalog', or 'blended'.
        """
        valid_modes = {"stock", "catalog", "blended"}
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode '{mode}'. Expected one of: {sorted(valid_modes)}")

        target_topic = self.CATALOG_TOPIC if mode == "catalog" else self.INVENTORY_TOPIC
        producer_config = config or ProducerConfig(
            topic=target_topic,
            client_id="inventory-producer",
        )

        super().__init__(
            config=producer_config,
            schema_registry_manager=schema_registry_manager,
            serializer=serializer,
            producer=producer,
        )

        self.mode = mode
        self.generator = generator or SyntheticDataGenerator()

    def generate_stock_event(self) -> dict[str, Any]:
        """Generate an inventory stock update event payload.

        Returns:
            Stock update event matching 'stock_updates.avsc' schema.
        """
        return self.generator.generate_stock_update()

    def generate_catalog_event(self) -> dict[str, Any]:
        """Generate a merchant catalog change event payload.

        Returns:
            Catalog change event matching 'catalog_changes.avsc' schema.
        """
        return self.generator.generate_catalog_change()

    def generate_event(self) -> dict[str, Any]:
        """Generate a domain event based on the configured mode.

        Returns:
            Stock update or catalog change event dictionary.
        """
        if self.mode == "stock":
            return self.generate_stock_event()
        if self.mode == "catalog":
            return self.generate_catalog_event()

        # Blended mode: 80% stock adjustments/reservations, 20% catalog edits
        if random.random() < 0.80:
            return self.generate_stock_event()
        return self.generate_catalog_event()

    def produce_stock_update(self, event: dict[str, Any] | None = None) -> bool:
        """Produce an inventory stock update event to 'marketplace.inventory'.

        Args:
            event: Optional pre-generated stock update event.

        Returns:
            True if queued successfully, False otherwise.
        """
        payload = event or self.generate_stock_event()
        return super().produce_one(event=payload, topic=self.INVENTORY_TOPIC)

    def produce_catalog_change(self, event: dict[str, Any] | None = None) -> bool:
        """Produce a catalog change event to 'marketplace.catalog'.

        Args:
            event: Optional pre-generated catalog change event.

        Returns:
            True if queued successfully, False otherwise.
        """
        payload = event or self.generate_catalog_event()
        return super().produce_one(event=payload, topic=self.CATALOG_TOPIC)

    def produce_one(
        self,
        event: dict[str, Any] | None = None,
        topic: str | None = None,
    ) -> bool:
        """Produce a single event, automatically routing to the appropriate topic.

        Args:
            event: Optional event payload. Generated according to mode if None.
            topic: Optional explicit target topic. Defaults based on event payload or mode.

        Returns:
            True if queued successfully, False otherwise.
        """
        if topic is not None:
            return super().produce_one(event=event, topic=topic)

        if event is None:
            event = self.generate_event()

        event_type = event.get("event_type", "")
        if event_type.startswith("PRODUCT_") or event_type == "PRICE_CHANGED" or "sku" in event:
            target_topic = self.CATALOG_TOPIC
        else:
            target_topic = self.INVENTORY_TOPIC

        return super().produce_one(event=event, topic=target_topic)


if __name__ == "__main__":
    parser = build_producer_arg_parser(
        default_topic=InventoryProducer.INVENTORY_TOPIC,
        default_rate=5.0,
        description="Marketplace Inventory & Catalog Kafka Event Producer",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["stock", "catalog", "blended"],
        default="stock",
        help="Event generation mode: 'stock', 'catalog', or 'blended'",
    )

    parsed_args, _ = parser.parse_known_args()
    config = parse_producer_config(parser=parser)

    producer = InventoryProducer(config=config, mode=parsed_args.mode)
    producer.run()
