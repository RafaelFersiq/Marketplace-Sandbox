"""Payment lifecycle Kafka event producer.

Publishes PaymentUpdateEvent payloads to 'marketplace.payments' with realistic
gateway status transitions (AUTHORIZED -> APPROVED, or DECLINED/FAILED) and Avro wire-format serialization.
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

logger = logging.getLogger("producers.payment")


class PaymentProducer(BaseProducer):
    """Kafka event producer for payment processing lifecycle events."""

    PAYMENTS_TOPIC: str = "marketplace.payments"

    def __init__(
        self,
        config: ProducerConfig | None = None,
        generator: SyntheticDataGenerator | None = None,
        schema_registry_manager: SchemaRegistryManager | None = None,
        serializer: AvroSerializerDeserializer | None = None,
        producer: confluent_kafka.Producer | None = None,
    ) -> None:
        """Initialize PaymentProducer.

        Args:
            config: Optional ProducerConfig. Defaults to PAYMENTS_TOPIC config.
            generator: Optional SyntheticDataGenerator instance.
            schema_registry_manager: Optional SchemaRegistryManager instance.
            serializer: Optional AvroSerializerDeserializer instance.
            producer: Optional pre-configured confluent_kafka.Producer.
        """
        producer_config = config or ProducerConfig(
            topic=self.PAYMENTS_TOPIC,
            client_id="payment-producer",
        )

        super().__init__(
            config=producer_config,
            schema_registry_manager=schema_registry_manager,
            serializer=serializer,
            producer=producer,
        )

        self.generator = generator or SyntheticDataGenerator()

    def generate_event(self) -> dict[str, Any]:
        """Generate a single payment status update event.

        Returns:
            Payment status update event matching 'payment_updates.avsc'.
        """
        return self.generator.generate_payment_update()

    def simulate_payment_lifecycle(
        self,
        order_id: str,
        customer_id: str,
        amount: Decimal,
        payment_method: str = "CREDIT_CARD",
    ) -> list[dict[str, Any]]:
        """Simulate realistic status transitions for an order payment lifecycle.

        Generates an initial state (AUTHORIZED) followed by final settlement (APPROVED)
        or terminal failure (DECLINED / FAILED).

        Args:
            order_id: Target order UUID string.
            customer_id: Purchasing customer UUID string.
            amount: Monetary transaction amount with 4 decimal places.
            payment_method: Payment instrument (e.g. CREDIT_CARD, PIX, BOLETO, DEBIT_CARD).

        Returns:
            List of chronological PaymentUpdateEvent dictionaries.
        """
        payment_id = str(uuid.uuid4())
        txn_ref = f"TXN-{random.randint(10000000, 99999999)}"
        provider = random.choice(["STRIPE", "PAGSEGURO", "MOCK_GATEWAY"])
        installments = 1 if payment_method in ("PIX", "BOLETO", "DEBIT_CARD") else random.randint(1, 10)
        base_time = self.generator._now()

        events: list[dict[str, Any]] = []

        # Initial authorization state
        auth_event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "PAYMENT_STATUS_UPDATED",
            "timestamp": base_time,
            "payment_id": payment_id,
            "order_id": order_id,
            "customer_id": customer_id,
            "payment_method": payment_method,
            "payment_status": "AUTHORIZED",
            "amount": amount,
            "currency": "BRL",
            "gateway_provider": provider,
            "transaction_reference": txn_ref,
            "failure_reason": None,
            "installments": installments,
        }
        events.append(auth_event)

        # Final transition (simulated after 200ms to 1500ms)
        final_time = base_time + timedelta(milliseconds=random.randint(200, 1500))

        # 90% approval rate, 7% decline, 3% gateway failure
        outcome_roll = random.random()
        if outcome_roll < 0.90:
            final_status = "APPROVED"
            failure_reason = None
        elif outcome_roll < 0.97:
            final_status = "DECLINED"
            failure_reason = random.choice(["INSUFFICIENT_FUNDS", "CARD_EXPIRED", "FRAUD_SUSPICION"])
        else:
            final_status = "FAILED"
            failure_reason = "GATEWAY_TIMEOUT"

        final_event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "PAYMENT_STATUS_UPDATED",
            "timestamp": final_time,
            "payment_id": payment_id,
            "order_id": order_id,
            "customer_id": customer_id,
            "payment_method": payment_method,
            "payment_status": final_status,
            "amount": amount,
            "currency": "BRL",
            "gateway_provider": provider,
            "transaction_reference": txn_ref,
            "failure_reason": failure_reason,
            "installments": installments,
        }
        events.append(final_event)

        return events

    def produce_payment_update(self, event: dict[str, Any] | None = None) -> bool:
        """Produce a payment update event to 'marketplace.payments'.

        Args:
            event: Optional pre-generated payment update event.

        Returns:
            True if queued successfully, False otherwise.
        """
        payload = event or self.generate_event()
        return self.produce_one(event=payload, topic=self.PAYMENTS_TOPIC)

    def produce_payment_lifecycle(
        self,
        order_id: str,
        customer_id: str,
        amount: Decimal,
        payment_method: str = "CREDIT_CARD",
    ) -> list[bool]:
        """Produce all events belonging to a simulated payment lifecycle sequence.

        Args:
            order_id: Associated order identifier.
            customer_id: Purchasing customer identifier.
            amount: Transaction monetary amount.
            payment_method: Payment instrument type.

        Returns:
            List of boolean delivery queue results for each lifecycle event.
        """
        events = self.simulate_payment_lifecycle(
            order_id=order_id,
            customer_id=customer_id,
            amount=amount,
            payment_method=payment_method,
        )
        return [self.produce_payment_update(event=ev) for ev in events]


if __name__ == "__main__":
    parser = build_producer_arg_parser(
        default_topic=PaymentProducer.PAYMENTS_TOPIC,
        default_rate=5.0,
        description="Marketplace Payment Status Kafka Event Producer",
    )
    config = parse_producer_config(parser=parser)
    producer = PaymentProducer(config=config)
    producer.run()
