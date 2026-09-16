"""Producer configuration and CLI argument parsing utilities.

Defines ProducerConfig dataclass and helper routines to configure Kafka brokers,
Confluent Schema Registry connections, rate limiting, and execution runtimes.
"""

import argparse
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_TOPIC_SUBJECT_MAP: dict[str, str] = {
    "marketplace.cart": "marketplace.cart-value",
    "marketplace.catalog": "marketplace.catalog-value",
    "marketplace.orders": "marketplace.orders-value",
    "marketplace.payments": "marketplace.payments-value",
    "marketplace.inventory": "marketplace.inventory-value",
    "marketplace.navigation": "marketplace.navigation-value",
}

DEFAULT_TOPIC_SCHEMA_STEM_MAP: dict[str, str] = {
    "marketplace.cart": "cart_events",
    "marketplace.catalog": "catalog_changes",
    "marketplace.orders": "order_creation",
    "marketplace.payments": "payment_updates",
    "marketplace.inventory": "stock_updates",
    "marketplace.navigation": "user_navigation",
}


@dataclass
class ProducerConfig:
    """Configuration parameters for Kafka event producers."""

    topic: str
    subject: str = ""
    schema_path: Path | str | None = None
    bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    schema_registry_url: str = field(default_factory=lambda: os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    events_per_second: float = 5.0
    max_events: int | None = None
    runtime_seconds: float | None = None
    client_id: str = "marketplace-producer"
    acks: str = "all"
    retries: int = 5
    retry_backoff_ms: int = 100
    compression_type: str = "snappy"
    extra_kafka_config: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Derive default subject name and convert schema path if omitted."""
        if not self.subject:
            self.subject = DEFAULT_TOPIC_SUBJECT_MAP.get(self.topic, f"{self.topic}-value")
        if self.schema_path is not None and isinstance(self.schema_path, str):
            self.schema_path = Path(self.schema_path)

    def to_kafka_config(self) -> dict[str, Any]:
        """Convert configuration to confluent-kafka client dictionary.

        Returns:
            Dictionary formatted for confluent_kafka.Producer.
        """
        base_config: dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": self.acks,
            "retries": self.retries,
            "retry.backoff.ms": self.retry_backoff_ms,
            "compression.type": self.compression_type,
        }
        base_config.update(self.extra_kafka_config)
        return base_config


def build_producer_arg_parser(
    default_topic: str = "marketplace.orders",
    default_rate: float = 5.0,
    description: str = "Marketplace Kafka Event Producer",
) -> argparse.ArgumentParser:
    """Build a standard ArgumentParser for event producers.

    Args:
        default_topic: Default Kafka topic for the producer.
        default_rate: Default event production rate in events/second.
        description: CLI help description.

    Returns:
        Configured argparse.ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap broker addresses (comma-separated)",
    )
    parser.add_argument(
        "--schema-registry-url",
        type=str,
        default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        help="Confluent Schema Registry base URL",
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=default_topic,
        help="Target Kafka topic",
    )
    parser.add_argument(
        "--subject",
        type=str,
        default="",
        help="Confluent Schema Registry subject name (defaults to <topic>-value)",
    )
    parser.add_argument(
        "--schema-path",
        type=str,
        default=None,
        help="Path to local Avro schema (.avsc) file",
    )
    parser.add_argument(
        "--rate",
        dest="events_per_second",
        type=float,
        default=default_rate,
        help="Target event throughput rate (events per second)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Maximum total events to produce before exiting (infinite if not specified)",
    )
    parser.add_argument(
        "--runtime-seconds",
        type=float,
        default=None,
        help="Maximum execution runtime in seconds before exiting (infinite if not specified)",
    )
    parser.add_argument(
        "--client-id",
        type=str,
        default="marketplace-producer",
        help="Kafka producer client identifier",
    )
    parser.add_argument(
        "--acks",
        type=str,
        default="all",
        choices=["all", "1", "0", "-1"],
        help="Kafka producer acknowledgment durability level",
    )
    parser.add_argument(
        "--compression",
        dest="compression_type",
        type=str,
        default="snappy",
        choices=["snappy", "gzip", "lz4", "zstd", "none"],
        help="Kafka message compression codec",
    )
    return parser


def parse_producer_config(
    parser: argparse.ArgumentParser | None = None,
    args: list[str] | None = None,
) -> ProducerConfig:
    """Parse CLI arguments into a ProducerConfig instance.

    Args:
        parser: Optional custom ArgumentParser. If None, default parser is used.
        args: Optional list of argument strings (e.g. sys.argv[1:]).

    Returns:
        Populated ProducerConfig instance.
    """
    if parser is None:
        parser = build_producer_arg_parser()

    parsed = parser.parse_args(args=args)

    return ProducerConfig(
        topic=parsed.topic,
        subject=parsed.subject,
        schema_path=Path(parsed.schema_path) if parsed.schema_path else None,
        bootstrap_servers=parsed.bootstrap_servers,
        schema_registry_url=parsed.schema_registry_url,
        events_per_second=parsed.events_per_second,
        max_events=parsed.max_events,
        runtime_seconds=parsed.runtime_seconds,
        client_id=parsed.client_id,
        acks=parsed.acks,
        compression_type=parsed.compression_type,
    )
