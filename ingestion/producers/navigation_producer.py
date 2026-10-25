"""High-volume clickstream and navigation Kafka event producer.

Publishes UserNavigationEvent payloads to 'marketplace.navigation' simulating user sessions,
search queries, and page interactions with high throughput.
"""

import logging
import random
import uuid
from datetime import timedelta
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

logger = logging.getLogger("producers.navigation")


class NavigationProducer(BaseProducer):
    """Kafka event producer for high-volume user navigation clickstream telemetry."""

    NAVIGATION_TOPIC: str = "marketplace.navigation"

    def __init__(
        self,
        config: ProducerConfig | None = None,
        generator: SyntheticDataGenerator | None = None,
        schema_registry_manager: SchemaRegistryManager | None = None,
        serializer: AvroSerializerDeserializer | None = None,
        producer: confluent_kafka.Producer | None = None,
    ) -> None:
        """Initialize NavigationProducer.

        Args:
            config: Optional ProducerConfig. Defaults to NAVIGATION_TOPIC with 20.0 msg/s rate.
            generator: Optional SyntheticDataGenerator instance.
            schema_registry_manager: Optional SchemaRegistryManager instance.
            serializer: Optional AvroSerializerDeserializer instance.
            producer: Optional pre-configured confluent_kafka.Producer.
        """
        producer_config = config or ProducerConfig(
            topic=self.NAVIGATION_TOPIC,
            events_per_second=20.0,
            client_id="navigation-producer",
        )

        super().__init__(
            config=producer_config,
            schema_registry_manager=schema_registry_manager,
            serializer=serializer,
            producer=producer,
        )

        self.generator = generator or SyntheticDataGenerator()

    def generate_event(self) -> dict[str, Any]:
        """Generate a single user navigation clickstream event.

        Returns:
            User navigation event matching 'user_navigation.avsc'.
        """
        return self.generator.generate_user_navigation()

    def simulate_user_journey(
        self,
        session_id: str | None = None,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Generate a coherent sequence of clickstream events simulating a user journey.

        Typical journey:
        1. PAGE_VIEW: Home page
        2. SEARCH_QUERY: Keyword search
        3. CATEGORY_BROWSE: Category landing page
        4. PRODUCT_CLICK: Specific product detail view

        Args:
            session_id: Optional tracking session UUID. Generated if omitted.
            user_id: Optional user identifier. Randomly chosen or None (guest) if omitted.

        Returns:
            List of sequential UserNavigationEvent dictionaries sharing session context.
        """
        active_session_id = session_id or str(uuid.uuid4())
        device_type = random.choice(["DESKTOP", "MOBILE", "TABLET"])
        ip_addr = self.generator.fake.ipv4()
        user_agent = self.generator.fake.user_agent()

        if user_id is None and self.generator._customers and random.random() > 0.3:
            resolved_user_id: str | None = random.choice(self.generator._customers)["customer_id"]
        else:
            resolved_user_id = user_id

        category = random.choice(self.generator._categories) if self.generator._categories else None
        product = random.choice(self.generator._products) if self.generator._products else None
        search_terms = ["smartphone", "notebook i7", "tenis nike", "fone bluetooth", "monitor gamer"]
        search_term = random.choice(search_terms)

        base_time = self.generator._now()

        journey_steps = [
            {
                "event_type": "PAGE_VIEW",
                "page_url": "https://marketplace.com/home",
                "referrer_url": "https://google.com",
                "product_id": None,
                "category_id": None,
                "search_term": None,
                "delay_sec": 0,
            },
            {
                "event_type": "SEARCH_QUERY",
                "page_url": f"https://marketplace.com/search?q={search_term.replace(' ', '+')}",
                "referrer_url": "https://marketplace.com/home",
                "product_id": None,
                "category_id": None,
                "search_term": search_term,
                "delay_sec": random.randint(3, 8),
            },
            {
                "event_type": "CATEGORY_BROWSE",
                "page_url": f"https://marketplace.com/categories/{category['category_id']}"
                if category
                else "https://marketplace.com/categories",
                "referrer_url": f"https://marketplace.com/search?q={search_term.replace(' ', '+')}",
                "product_id": None,
                "category_id": category["category_id"] if category else None,
                "search_term": None,
                "delay_sec": random.randint(5, 15),
            },
            {
                "event_type": "PRODUCT_CLICK",
                "page_url": f"https://marketplace.com/products/{product['product_id']}"
                if product
                else "https://marketplace.com/products",
                "referrer_url": f"https://marketplace.com/categories/{category['category_id']}"
                if category
                else "https://marketplace.com/categories",
                "product_id": product["product_id"] if product else None,
                "category_id": None,
                "search_term": None,
                "delay_sec": random.randint(4, 12),
            },
        ]

        events: list[dict[str, Any]] = []
        cumulative_sec = 0

        for step in journey_steps:
            cumulative_sec += step["delay_sec"]
            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": step["event_type"],
                "timestamp": base_time + timedelta(seconds=cumulative_sec),
                "session_id": active_session_id,
                "user_id": resolved_user_id,
                "device_type": device_type,
                "ip_address": ip_addr,
                "user_agent": user_agent,
                "page_url": step["page_url"],
                "referrer_url": step["referrer_url"],
                "product_id": step["product_id"],
                "category_id": step["category_id"],
                "search_term": step["search_term"],
            }
            events.append(event)

        return events

    def produce_navigation_event(self, event: dict[str, Any] | None = None) -> bool:
        """Produce a user navigation clickstream event to 'marketplace.navigation'.

        Args:
            event: Optional pre-generated navigation event.

        Returns:
            True if queued successfully, False otherwise.
        """
        payload = event or self.generate_event()
        return self.produce_one(event=payload, topic=self.NAVIGATION_TOPIC)

    def produce_user_journey(
        self,
        session_id: str | None = None,
        user_id: str | None = None,
    ) -> list[bool]:
        """Produce all events belonging to a coherent user navigation journey.

        Args:
            session_id: Optional tracking session identifier.
            user_id: Optional user identifier.

        Returns:
            List of boolean delivery queue results for each journey event.
        """
        events = self.simulate_user_journey(session_id=session_id, user_id=user_id)
        return [self.produce_navigation_event(event=ev) for ev in events]


if __name__ == "__main__":
    parser = build_producer_arg_parser(
        default_topic=NavigationProducer.NAVIGATION_TOPIC,
        default_rate=20.0,
        description="Marketplace Navigation Clickstream Kafka Event Producer",
    )
    config = parse_producer_config(parser=parser)
    producer = NavigationProducer(config=config)
    producer.run()
