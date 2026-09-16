"""Marketplace Kafka Event Producers package.

Provides base producer abstractions, synthetic data generation, and domain-specific
event producers for streaming ingestion into Apache Kafka.
"""

from ingestion.producers.base_producer import BaseProducer, ProducerMetrics
from ingestion.producers.config import (
    ProducerConfig,
    build_producer_arg_parser,
    parse_producer_config,
)
from ingestion.producers.generator import SyntheticDataGenerator

__all__ = [
    "BaseProducer",
    "ProducerConfig",
    "ProducerMetrics",
    "SyntheticDataGenerator",
    "build_producer_arg_parser",
    "parse_producer_config",
]
