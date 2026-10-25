"""Anti-Fraud and Security Telemetry Kafka event producer.

Publishes security telemetry and synthetic suspicious activity patterns across marketplace event schemas:
- Rapid checkout frequency / velocity bursts on 'marketplace.orders'
- Unusual high-value orders on 'marketplace.orders'
- Geographical anomalies / IP hopping on 'marketplace.orders'
- Card testing / failed payment attempts on 'marketplace.payments'
"""

import logging
import random
import uuid
from datetime import timedelta
from decimal import Decimal
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

logger = logging.getLogger("producers.antifraud")

VALID_PATTERNS: set[str] = {"high_value", "velocity", "geo", "card_testing"}


class AntiFraudProducer(BaseProducer):
    """Kafka event producer emitting security telemetry and synthetic fraud patterns."""

    ORDERS_TOPIC: str = "marketplace.orders"
    PAYMENTS_TOPIC: str = "marketplace.payments"

    def __init__(
        self,
        config: ProducerConfig | None = None,
        pattern: str = "high_value",
        generator: SyntheticDataGenerator | None = None,
        schema_registry_manager: SchemaRegistryManager | None = None,
        serializer: AvroSerializerDeserializer | None = None,
        producer: confluent_kafka.Producer | None = None,
    ) -> None:
        """Initialize AntiFraudProducer.

        Args:
            config: Optional ProducerConfig. Defaults to ORDERS_TOPIC config.
            pattern: Default fraud pattern: 'high_value', 'velocity', 'geo', or 'card_testing'.
            generator: Optional SyntheticDataGenerator instance.
            schema_registry_manager: Optional SchemaRegistryManager instance.
            serializer: Optional AvroSerializerDeserializer instance.
            producer: Optional pre-configured confluent_kafka.Producer.

        Raises:
            ValueError: If pattern is not one of the supported patterns.
        """
        if pattern not in VALID_PATTERNS:
            raise ValueError(f"Invalid pattern '{pattern}'. Expected one of: {sorted(VALID_PATTERNS)}")

        target_topic = self.PAYMENTS_TOPIC if pattern == "card_testing" else self.ORDERS_TOPIC
        producer_config = config or ProducerConfig(
            topic=target_topic,
            client_id="antifraud-producer",
        )

        super().__init__(
            config=producer_config,
            schema_registry_manager=schema_registry_manager,
            serializer=serializer,
            producer=producer,
        )

        self.pattern = pattern
        self.generator = generator or SyntheticDataGenerator()

    def generate_high_value_order(self, multiplier: float = 10.0) -> dict[str, Any]:
        """Generate an order event with an unusually high total value.

        Simulates fraudulent high-ticket purchases (e.g. bulk electronics, luxury items)
        exceeding normal retail thresholds.

        Args:
            multiplier: Scaling factor for item prices. Defaults to 10.0.

        Returns:
            Order creation event dictionary matching 'order_creation.avsc'.

        Raises:
            ValueError: If multiplier is not positive.
        """
        if multiplier <= 0:
            raise ValueError(f"Multiplier must be positive, got {multiplier}")

        mult = Decimal(str(multiplier))
        customer = random.choice(self.generator._customers)
        seller = random.choice(self.generator._sellers)

        # Select high-ticket products (e.g., iPhone 15 Pro, Dell XPS 13, Galaxy S24)
        expensive_prods = [p for p in self.generator._products if p["price"] >= Decimal("4500.0000")]
        if not expensive_prods:
            expensive_prods = self.generator._products

        selected_prod = random.choice(expensive_prods)
        unit_price = (selected_prod["price"] * mult).quantize(Decimal("0.0001"))
        qty = 1
        item_subtotal = (unit_price * Decimal(qty)).quantize(Decimal("0.0001"))

        items = [
            {
                "item_id": str(uuid.uuid4()),
                "product_id": selected_prod["product_id"],
                "product_name": selected_prod["product_name"],
                "quantity": qty,
                "unit_price": unit_price,
                "subtotal": item_subtotal,
            }
        ]

        shipping = Decimal("150.0000")
        discount = Decimal("0.0000")
        total = (item_subtotal + shipping - discount).quantize(Decimal("0.0001"))

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "ORDER_CREATED",
            "timestamp": self.generator._now(),
            "order_id": str(uuid.uuid4()),
            "customer_id": customer["customer_id"],
            "seller_id": seller["seller_id"],
            "order_status": "CREATED",
            "currency": "BRL",
            "subtotal_amount": item_subtotal,
            "discount_amount": discount,
            "shipping_fee": shipping,
            "total_amount": total,
            "payment_method": "CREDIT_CARD",
            "shipping_address": {
                "street": customer["address"]["street"],
                "number": customer["address"]["number"],
                "complement": customer["address"]["complement"],
                "city": customer["address"]["city"],
                "state": customer["address"]["state"],
                "zip_code": customer["address"]["zip_code"],
                "country": customer["address"]["country"],
            },
            "items": items,
        }

    def generate_velocity_burst(
        self,
        customer_id: str | None = None,
        count: int = 5,
    ) -> list[dict[str, Any]]:
        """Generate a burst of order events for the same customer within seconds.

        Simulates rapid automated checkout scripts (botting) or credential stuffing checkout bursts.

        Args:
            customer_id: Optional customer UUID. If None, selects an existing customer.
            count: Number of rapid orders to generate in the burst. Defaults to 5.

        Returns:
            List of chronologically spaced order creation events.

        Raises:
            ValueError: If count is less than 1.
        """
        if count < 1:
            raise ValueError(f"Count must be at least 1, got {count}")

        cust_id = customer_id or random.choice(self.generator._customers)["customer_id"]
        customer = next((c for c in self.generator._customers if c["customer_id"] == cust_id), None)
        shipping_addr = customer["address"] if customer else None

        base_time = self.generator._now()
        orders: list[dict[str, Any]] = []

        for i in range(count):
            order = self.generator.generate_order_creation()
            order["customer_id"] = cust_id
            order["order_id"] = str(uuid.uuid4())
            order["event_id"] = str(uuid.uuid4())
            order["timestamp"] = base_time + timedelta(milliseconds=i * 250)
            if shipping_addr:
                order["shipping_address"] = {
                    "street": shipping_addr["street"],
                    "number": shipping_addr["number"],
                    "complement": shipping_addr.get("complement"),
                    "city": shipping_addr["city"],
                    "state": shipping_addr["state"],
                    "zip_code": shipping_addr["zip_code"],
                    "country": shipping_addr["country"],
                }
            orders.append(order)

        return orders

    def generate_geo_anomaly(
        self,
        customer_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Generate subsequent orders under the same customer originating from distant geographical states.

        Simulates account takeover or impossible travel (e.g. orders placed in SP and AM within seconds).

        Args:
            customer_id: Optional customer UUID. If None, selects an existing customer.

        Returns:
            List of 2 chronologically adjacent order events with conflicting destination states.
        """
        cust_id = customer_id or random.choice(self.generator._customers)["customer_id"]
        base_time = self.generator._now()

        # First order in São Paulo (SP)
        order_sp = self.generator.generate_order_creation()
        order_sp["customer_id"] = cust_id
        order_sp["order_id"] = str(uuid.uuid4())
        order_sp["event_id"] = str(uuid.uuid4())
        order_sp["timestamp"] = base_time
        order_sp["shipping_address"] = {
            "street": "Avenida Paulista",
            "number": "1000",
            "complement": "Andar 10",
            "city": "São Paulo",
            "state": "SP",
            "zip_code": "01310100",
            "country": "BR",
        }

        # Second order in Amazonas (AM) 2 seconds later (geographically impossible travel)
        order_am = self.generator.generate_order_creation()
        order_am["customer_id"] = cust_id
        order_am["order_id"] = str(uuid.uuid4())
        order_am["event_id"] = str(uuid.uuid4())
        order_am["timestamp"] = base_time + timedelta(seconds=2)
        order_am["shipping_address"] = {
            "street": "Avenida Djalma Batista",
            "number": "500",
            "complement": None,
            "city": "Manaus",
            "state": "AM",
            "zip_code": "69050010",
            "country": "BR",
        }

        return [order_sp, order_am]

    def generate_card_testing_pattern(
        self,
        order_id: str | None = None,
        customer_id: str | None = None,
        attempts: int = 3,
    ) -> list[dict[str, Any]]:
        """Generate multiple failed payment attempts for an order with different transaction references.

        Simulates card testing fraud where automated actors iterate stolen payment cards.

        Args:
            order_id: Optional target order UUID. Generated if None.
            customer_id: Optional customer UUID. Selected from pool if None.
            attempts: Number of failed payment attempts to generate. Defaults to 3.

        Returns:
            List of failed/declined PaymentUpdateEvent dictionaries.

        Raises:
            ValueError: If attempts is less than 1.
        """
        if attempts < 1:
            raise ValueError(f"Attempts must be at least 1, got {attempts}")

        target_order_id = order_id or str(uuid.uuid4())
        target_customer_id = customer_id or random.choice(self.generator._customers)["customer_id"]
        base_time = self.generator._now()
        amount = Decimal(str(random.randint(100, 1500))).quantize(Decimal("0.0001"))

        events: list[dict[str, Any]] = []
        failure_reasons = ["INSUFFICIENT_FUNDS", "FRAUD_SUSPICION", "CARD_EXPIRED", "GATEWAY_TIMEOUT"]
        gateway_providers = ["STRIPE", "PAGSEGURO", "MOCK_GATEWAY"]

        for i in range(attempts):
            status = random.choice(["DECLINED", "FAILED"])
            reason = random.choice(failure_reasons)
            provider = random.choice(gateway_providers)
            txn_ref = f"TXN-FRAUD-{uuid.uuid4().hex[:8].upper()}"

            events.append(
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "PAYMENT_STATUS_UPDATED",
                    "timestamp": base_time + timedelta(milliseconds=i * 500),
                    "payment_id": str(uuid.uuid4()),
                    "order_id": target_order_id,
                    "customer_id": target_customer_id,
                    "payment_method": "CREDIT_CARD",
                    "payment_status": status,
                    "amount": amount,
                    "currency": "BRL",
                    "gateway_provider": provider,
                    "transaction_reference": txn_ref,
                    "failure_reason": reason,
                    "installments": 1,
                }
            )

        return events

    def generate_event(self) -> dict[str, Any]:
        """Generate a single domain event based on the configured pattern.

        Satisfies 'order_creation.avsc' schema contract.

        Returns:
            Anomalous order creation event dictionary.
        """
        if self.pattern == "high_value":
            return self.generate_high_value_order()
        if self.pattern == "velocity":
            return self.generate_velocity_burst(count=1)[0]
        if self.pattern == "geo":
            return self.generate_geo_anomaly()[0]

        # For card_testing or fallback, randomly select an anomalous order generator
        choices = [
            lambda: self.generate_high_value_order(),
            lambda: self.generate_velocity_burst(count=1)[0],
            lambda: self.generate_geo_anomaly()[0],
        ]
        return random.choice(choices)()

    def produce_high_value_order(self, multiplier: float = 10.0) -> bool:
        """Produce a high-value order anomaly event to 'marketplace.orders'.

        Args:
            multiplier: Scaling factor for item prices.

        Returns:
            True if queued successfully, False otherwise.
        """
        order = self.generate_high_value_order(multiplier=multiplier)
        logger.info(
            "Produced high-value order anomaly",
            extra={
                "order_id": order["order_id"],
                "total_amount": str(order["total_amount"]),
                "pattern": "high_value",
            },
        )
        return self.produce_one(event=order, topic=self.ORDERS_TOPIC)

    def produce_velocity_burst(
        self,
        customer_id: str | None = None,
        count: int = 5,
    ) -> list[bool]:
        """Produce a rapid checkout frequency burst to 'marketplace.orders'.

        Args:
            customer_id: Optional target customer UUID.
            count: Number of burst events.

        Returns:
            List of delivery queue boolean results.
        """
        orders = self.generate_velocity_burst(customer_id=customer_id, count=count)
        logger.info(
            "Producing velocity burst anomaly",
            extra={"count": count, "pattern": "velocity"},
        )
        return [self.produce_one(event=o, topic=self.ORDERS_TOPIC) for o in orders]

    def produce_geo_anomaly(
        self,
        customer_id: str | None = None,
    ) -> list[bool]:
        """Produce geographically conflicting orders to 'marketplace.orders'.

        Args:
            customer_id: Optional target customer UUID.

        Returns:
            List of delivery queue boolean results.
        """
        geo_orders = self.generate_geo_anomaly(customer_id=customer_id)
        logger.info(
            "Producing geographical anomaly sequence",
            extra={"pattern": "geo", "states": ["SP", "AM"]},
        )
        return [self.produce_one(event=o, topic=self.ORDERS_TOPIC) for o in geo_orders]

    def produce_card_testing(
        self,
        order_id: str | None = None,
        customer_id: str | None = None,
        attempts: int = 3,
    ) -> list[bool]:
        """Produce card testing payment failure sequence to 'marketplace.payments'.

        Args:
            order_id: Optional target order UUID.
            customer_id: Optional customer UUID.
            attempts: Number of failed payment attempts.

        Returns:
            List of delivery queue boolean results.
        """
        payment_events = self.generate_card_testing_pattern(
            order_id=order_id,
            customer_id=customer_id,
            attempts=attempts,
        )
        logger.info(
            "Producing card testing payment anomaly",
            extra={"attempts": attempts, "pattern": "card_testing"},
        )
        return [self.produce_one(event=p, topic=self.PAYMENTS_TOPIC) for p in payment_events]

    def produce_anomaly(self, pattern: str = "high_value") -> bool | list[bool]:
        """Produce the selected suspicious fraud activity pattern to Kafka.

        Args:
            pattern: Fraud pattern identifier: 'high_value', 'velocity', 'geo', or 'card_testing'.

        Returns:
            Boolean for single-event pattern or list of booleans for burst patterns.

        Raises:
            ValueError: If pattern is not recognized.
        """
        if pattern not in VALID_PATTERNS:
            raise ValueError(f"Invalid pattern '{pattern}'. Expected one of: {sorted(VALID_PATTERNS)}")

        if pattern == "high_value":
            return self.produce_high_value_order()
        if pattern == "velocity":
            return self.produce_velocity_burst()
        if pattern == "geo":
            return self.produce_geo_anomaly()
        if pattern == "card_testing":
            return self.produce_card_testing()

        raise ValueError(f"Unsupported pattern '{pattern}'")

    def produce_one(
        self,
        event: dict[str, Any] | None = None,
        topic: str | None = None,
    ) -> bool:
        """Produce a single event, automatically routing to the appropriate Kafka topic.

        Args:
            event: Optional event payload. Generated according to pattern if None.
            topic: Optional explicit target topic. Defaults based on event payload or config.

        Returns:
            True if queued successfully, False otherwise.
        """
        if topic is not None:
            return super().produce_one(event=event, topic=topic)

        if event is None:
            event = self.generate_event()

        event_type = event.get("event_type", "")
        target_topic = self.PAYMENTS_TOPIC if event_type == "PAYMENT_STATUS_UPDATED" else self.ORDERS_TOPIC
        return super().produce_one(event=event, topic=target_topic)


if __name__ == "__main__":
    parser = build_producer_arg_parser(
        default_topic=AntiFraudProducer.ORDERS_TOPIC,
        default_rate=2.0,
        description="Marketplace Anti-Fraud & Security Telemetry Kafka Event Producer",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        choices=["high_value", "velocity", "geo", "card_testing"],
        default="high_value",
        help="Suspicious activity pattern: 'high_value', 'velocity', 'geo', or 'card_testing'",
    )

    parsed_args, _ = parser.parse_known_args()
    config = parse_producer_config(parser=parser)

    producer = AntiFraudProducer(config=config, pattern=parsed_args.pattern)
    producer.run()
