"""Marketplace Kafka Event Producers package.

Provides base producer abstractions, synthetic data generation, chaos resilience simulation,
and domain-specific event producers for streaming ingestion into Apache Kafka.
"""

from ingestion.producers.antifraud_producer import AntiFraudProducer
from ingestion.producers.base_producer import BaseProducer, ProducerMetrics
from ingestion.producers.chaos import (
    ChaosConfig,
    ChaosEngine,
    ChaosMetrics,
    SchemaViolationType,
)
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

__all__ = [
    "AntiFraudProducer",
    "BaseProducer",
    "ChaosConfig",
    "ChaosEngine",
    "ChaosMetrics",
    "CheckoutProducer",
    "InventoryProducer",
    "NavigationProducer",
    "PaymentProducer",
    "ProducerConfig",
    "ProducerMetrics",
    "SchemaViolationType",
    "SyntheticDataGenerator",
    "build_producer_arg_parser",
    "parse_producer_config",
]
