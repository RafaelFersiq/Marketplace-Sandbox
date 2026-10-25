"""Checkout and Shopping Cart Kafka event producer.

Publishes OrderCreationEvent to 'marketplace.orders' and CartEvent to 'marketplace.cart'
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

logger = logging.getLogger("producers.checkout")


class CheckoutProducer(BaseProducer):
    """Kafka event producer for checkout order creations and shopping cart activity."""

    ORDERS_TOPIC: str = "marketplace.orders"
    CART_TOPIC: str = "marketplace.cart"

    def __init__(
        self,
        config: ProducerConfig | None = None,
        mode: str = "orders",
        generator: SyntheticDataGenerator | None = None,
        schema_registry_manager: SchemaRegistryManager | None = None,
        serializer: AvroSerializerDeserializer | None = None,
        producer: confluent_kafka.Producer | None = None,
    ) -> None:
        """Initialize CheckoutProducer.

        Args:
            config: Optional ProducerConfig. Defaults to ORDERS_TOPIC config.
            mode: Event production mode: 'orders', 'cart', or 'blended'.
            generator: Optional SyntheticDataGenerator instance.
            schema_registry_manager: Optional SchemaRegistryManager instance.
            serializer: Optional AvroSerializerDeserializer instance.
            producer: Optional pre-configured confluent_kafka.Producer.

        Raises:
            ValueError: If mode is not one of 'orders', 'cart', or 'blended'.
        """
        valid_modes = {"orders", "cart", "blended"}
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode '{mode}'. Expected one of: {sorted(valid_modes)}")

        target_topic = self.CART_TOPIC if mode == "cart" else self.ORDERS_TOPIC
        producer_config = config or ProducerConfig(
            topic=target_topic,
            client_id="checkout-producer",
        )

        super().__init__(
            config=producer_config,
            schema_registry_manager=schema_registry_manager,
            serializer=serializer,
            producer=producer,
        )

        self.mode = mode
        self.generator = generator or SyntheticDataGenerator()

    def generate_order_event(self) -> dict[str, Any]:
        """Generate an order creation event payload.

        Returns:
            Order creation event matching 'order_creation.avsc' schema.
        """
        return self.generator.generate_order_creation()

    def generate_cart_event(self) -> dict[str, Any]:
        """Generate a shopping cart event payload.

        Returns:
            Shopping cart event matching 'cart_events.avsc' schema.
        """
        return self.generator.generate_cart_event()

    def generate_event(self) -> dict[str, Any]:
        """Generate a domain event based on the configured mode.

        Returns:
            Order creation or cart event dictionary.
        """
        if self.mode == "orders":
            return self.generate_order_event()
        if self.mode == "cart":
            return self.generate_cart_event()

        # Blended mode: 70% cart events, 30% order events (realistic funnel)
        if random.random() < 0.70:
            return self.generate_cart_event()
        return self.generate_order_event()

    def produce_order(self, event: dict[str, Any] | None = None) -> bool:
        """Produce an order creation event to 'marketplace.orders'.

        Args:
            event: Optional pre-generated order creation event.

        Returns:
            True if queued successfully, False otherwise.
        """
        payload = event or self.generate_order_event()
        return super().produce_one(event=payload, topic=self.ORDERS_TOPIC)

    def produce_cart(self, event: dict[str, Any] | None = None) -> bool:
        """Produce a cart event to 'marketplace.cart'.

        Args:
            event: Optional pre-generated cart event.

        Returns:
            True if queued successfully, False otherwise.
        """
        payload = event or self.generate_cart_event()
        return super().produce_one(event=payload, topic=self.CART_TOPIC)

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
        target_topic = self.CART_TOPIC if event_type.startswith("CART_") or "cart_id" in event else self.ORDERS_TOPIC
        return super().produce_one(event=event, topic=target_topic)


if __name__ == "__main__":
    parser = build_producer_arg_parser(
        default_topic=CheckoutProducer.ORDERS_TOPIC,
        default_rate=5.0,
        description="Marketplace Checkout & Cart Kafka Event Producer",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["orders", "cart", "blended"],
        default="orders",
        help="Event generation mode: 'orders', 'cart', or 'blended'",
    )

    parsed_args, _ = parser.parse_known_args()
    config = parse_producer_config(parser=parser)

    producer = CheckoutProducer(config=config, mode=parsed_args.mode)
    producer.run()
