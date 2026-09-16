"""Base Kafka event producer abstraction for marketplace streaming ingestion.

Provides BaseProducer class integrating confluent-kafka Producer, Confluent Schema Registry,
Avro wire-format serialization, throughput rate limiting, delivery callbacks, and graceful shutdown.
"""

import json
import logging
import signal
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any

import confluent_kafka

from ingestion.producers.config import DEFAULT_TOPIC_SCHEMA_STEM_MAP, ProducerConfig
from ingestion.schema_registry import SchemaRegistryManager
from ingestion.serialization import AvroSerializerDeserializer, SerializationError

logger = logging.getLogger("producers.base")


@dataclass
class ProducerMetrics:
    """Telemetry and counters for producer operations."""

    produced_count: int = 0
    delivered_count: int = 0
    error_count: int = 0
    start_time: float = 0.0
    end_time: float | None = None

    @property
    def elapsed_seconds(self) -> float:
        """Calculate total runtime in seconds."""
        if self.start_time == 0.0:
            return 0.0
        current = self.end_time if self.end_time is not None else time.monotonic()
        return max(0.0, current - self.start_time)

    @property
    def effective_throughput(self) -> float:
        """Calculate effective delivery throughput (delivered/sec)."""
        elapsed = self.elapsed_seconds
        if elapsed <= 0.0:
            return 0.0
        return self.delivered_count / elapsed


class BaseProducer(ABC):
    """Abstract base producer for Kafka event streams with Avro schema registry validation."""

    def __init__(
        self,
        config: ProducerConfig,
        schema_registry_manager: SchemaRegistryManager | None = None,
        serializer: AvroSerializerDeserializer | None = None,
        producer: confluent_kafka.Producer | None = None,
    ) -> None:
        """Initialize the BaseProducer.

        Args:
            config: Producer configuration settings.
            schema_registry_manager: Optional SchemaRegistryManager instance.
            serializer: Optional AvroSerializerDeserializer instance.
            producer: Optional pre-configured confluent_kafka.Producer (useful for mocking/testing).
        """
        self.config = config
        self.schema_registry = schema_registry_manager or SchemaRegistryManager(url=config.schema_registry_url)
        self.serializer = serializer or AvroSerializerDeserializer(schema_registry_manager=self.schema_registry)
        self.producer = producer
        self.schema_dict: dict[str, Any] = {}
        self.schema_id: int | None = None
        self.is_running: bool = False
        self.metrics = ProducerMetrics()

    def _resolve_schema_path(self) -> Path:
        """Locate the Avro schema file (.avsc) for this producer."""
        if self.config.schema_path is not None:
            path = Path(self.config.schema_path)
            if not path.is_file():
                raise FileNotFoundError(f"Configured schema file not found at: {path}")
            return path

        stem = DEFAULT_TOPIC_SCHEMA_STEM_MAP.get(self.config.topic)
        if not stem:
            # Fallback based on topic suffix (e.g. marketplace.orders -> order_creation or orders)
            topic_clean = self.config.topic.split(".")[-1]
            stem = topic_clean

        schema_dir = Path(__file__).resolve().parent.parent / "schemas" / "avro"
        candidate = schema_dir / f"{stem}.avsc"
        if candidate.is_file():
            return candidate

        # Try searching by matching stem prefix
        for file in schema_dir.glob("*.avsc"):
            if stem in file.stem or file.stem in stem:
                return file

        raise FileNotFoundError(
            f"Could not automatically resolve schema file for topic '{self.config.topic}' in {schema_dir}"
        )

    def load_schema(self) -> dict[str, Any]:
        """Load and parse local Avro schema JSON file.

        Returns:
            Avro schema dictionary.
        """
        schema_path = self._resolve_schema_path()
        with schema_path.open("r", encoding="utf-8") as f:
            self.schema_dict = json.load(f)
        return self.schema_dict

    def setup(self) -> None:
        """Prepare producer: load schema, resolve Schema Registry ID, and create Kafka producer."""
        if not self.schema_dict:
            self.load_schema()

        # Check / register schema with Schema Registry
        subject = self.config.subject
        try:
            schema_str = json.dumps(self.schema_dict)
            self.schema_id = self.schema_registry.register_schema(
                subject=subject,
                schema=schema_str,
                schema_type="AVRO",
            )
            logger.info(
                "Registered schema for subject '%s' with ID %d",
                subject,
                self.schema_id,
                extra={"subject": subject, "schema_id": self.schema_id},
            )
        except Exception as exc:
            logger.warning(
                "Could not register schema with Schema Registry (%s). Attempting local/fallback resolution: %s",
                self.config.schema_registry_url,
                exc,
            )
            # Default to dummy/mock schema_id if registry is unreachable in standalone/offline tests
            if self.schema_id is None:
                self.schema_id = 1

        # Instantiate Kafka Producer if not injected
        if self.producer is None:
            kafka_config = self.config.to_kafka_config()
            self.producer = confluent_kafka.Producer(kafka_config)
            logger.info(
                "Initialized Kafka Producer for topic '%s' on %s",
                self.config.topic,
                self.config.bootstrap_servers,
            )

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Callback invoked by confluent-kafka on delivery success or failure.

        Args:
            err: KafkaError or None if message was successfully delivered.
            msg: Message object containing partition, offset, and key/value metadata.
        """
        if err is not None:
            self.metrics.error_count += 1
            logger.error(
                "Message delivery failed on topic %s: %s",
                msg.topic() if msg else self.config.topic,
                err,
                extra={"topic": self.config.topic, "error": str(err)},
            )
        else:
            self.metrics.delivered_count += 1
            logger.debug(
                "Message delivered to %s [%d] at offset %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                },
            )

    @abstractmethod
    def generate_event(self) -> dict[str, Any]:
        """Generate a single domain event dictionary.

        Returns:
            Dictionary payload matching the domain Avro schema.
        """
        raise NotImplementedError("Subclasses must implement generate_event()")

    def get_event_key(self, event: dict[str, Any]) -> str | bytes | None:
        """Extract partition key from the generated event dictionary.

        Args:
            event: Event data dictionary.

        Returns:
            Partition key string, bytes, or None.
        """
        key_candidates = [
            "order_id",
            "payment_id",
            "cart_id",
            "session_id",
            "inventory_id",
            "product_id",
            "event_id",
        ]
        for key in key_candidates:
            if key in event and event[key] is not None:
                return str(event[key])
        return None

    def produce_one(self, event: dict[str, Any] | None = None) -> bool:
        """Serialize and produce a single event to the configured Kafka topic.

        Args:
            event: Optional pre-generated event. If None, generate_event() is called.

        Returns:
            True if message was queued for delivery, False otherwise.
        """
        if self.producer is None or not self.schema_dict or self.schema_id is None:
            self.setup()

        if event is None:
            event = self.generate_event()

        assert self.producer is not None
        assert self.schema_id is not None

        try:
            payload_bytes = self.serializer.serialize(
                payload=event,
                schema=self.schema_dict,
                schema_id=self.schema_id,
                include_wire_header=True,
            )
        except SerializationError as exc:
            self.metrics.error_count += 1
            logger.error("Failed to serialize event for topic %s: %s", self.config.topic, exc)
            return False

        key = self.get_event_key(event)
        key_bytes = key.encode("utf-8") if isinstance(key, str) else key

        try:
            self.producer.produce(
                topic=self.config.topic,
                key=key_bytes,
                value=payload_bytes,
                on_delivery=self._delivery_callback,
            )
            self.producer.poll(0)
            self.metrics.produced_count += 1
            return True
        except confluent_kafka.KafkaException as exc:
            self.metrics.error_count += 1
            logger.error("Kafka produce error on topic %s: %s", self.config.topic, exc)
            return False
        except BufferError:
            logger.warning("Kafka local queue full; waiting for queue clearance.")
            self.producer.poll(1.0)
            self.producer.produce(
                topic=self.config.topic,
                key=key_bytes,
                value=payload_bytes,
                on_delivery=self._delivery_callback,
            )
            self.metrics.produced_count += 1
            return True

    def flush(self, timeout: float = 10.0) -> int:
        """Flush pending Kafka producer queue.

        Args:
            timeout: Maximum seconds to wait for delivery callbacks.

        Returns:
            Number of un-delivered messages still in queue.
        """
        if self.producer is not None:
            return self.producer.flush(timeout=timeout)
        return 0

    def close(self) -> None:
        """Stop producer loop and flush remaining messages."""
        self.is_running = False
        self.flush()
        self.metrics.end_time = time.monotonic()
        logger.info(
            "Producer stopped for topic '%s'. Produced: %d, Delivered: %d, Errors: %d",
            self.config.topic,
            self.metrics.produced_count,
            self.metrics.delivered_count,
            self.metrics.error_count,
            extra={
                "topic": self.config.topic,
                "produced": self.metrics.produced_count,
                "delivered": self.metrics.delivered_count,
                "errors": self.metrics.error_count,
            },
        )

    def run(self) -> None:
        """Execute the production loop with rate limiting and graceful shutdown."""
        self.setup()
        self.is_running = True
        self.metrics.start_time = time.monotonic()

        def _signal_handler(_sig: int, _frame: FrameType | None) -> None:
            logger.info("Received termination signal. Stopping producer...")
            self.is_running = False

        try:
            signal.signal(signal.SIGINT, _signal_handler)
            signal.signal(signal.SIGTERM, _signal_handler)
        except (ValueError, AttributeError):
            # Not in main thread, ignore signal attachment
            pass

        rate = self.config.events_per_second
        target_interval = 1.0 / rate if rate > 0 else 0.0

        logger.info(
            "Starting producer on topic '%s' (rate=%.1f msg/s, max_events=%s, max_runtime=%ss)",
            self.config.topic,
            rate,
            self.config.max_events,
            self.config.runtime_seconds,
        )

        try:
            while self.is_running:
                loop_start = time.monotonic()

                # Check max events limit
                if self.config.max_events is not None and self.metrics.produced_count >= self.config.max_events:
                    logger.info("Reached target max_events limit (%d). Exiting loop.", self.config.max_events)
                    break

                # Check max runtime limit
                if (
                    self.config.runtime_seconds is not None
                    and self.metrics.elapsed_seconds >= self.config.runtime_seconds
                ):
                    logger.info(
                        "Reached runtime limit (%.1fs). Exiting loop.",
                        self.config.runtime_seconds,
                    )
                    break

                self.produce_one()

                # Rate limiting sleep
                elapsed = time.monotonic() - loop_start
                sleep_time = target_interval - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

        finally:
            self.close()

    def __enter__(self) -> "BaseProducer":
        """Context manager entry."""
        self.setup()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
