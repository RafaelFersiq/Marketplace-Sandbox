"""Spark Structured Streaming configuration and CLI argument parsing utilities.

Defines SparkStreamingConfig dataclass and helper routines to configure Kafka brokers,
Schema Registry, MinIO/S3A storage, streaming triggers, watermarking, and fault-tolerant
checkpointing for the real-time marketplace ingestion pipeline.
"""

import argparse
import os
from dataclasses import dataclass, field

DEFAULT_STREAMING_TOPICS: tuple[str, ...] = (
    "marketplace.orders",
    "marketplace.payments",
    "marketplace.cart",
    "marketplace.inventory",
    "marketplace.catalog",
    "marketplace.navigation",
)
DEFAULT_DLQ_TOPIC: str = "marketplace.dlq"
DEFAULT_SPARK_PACKAGES: str = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.apache.spark:spark-avro_2.12:3.5.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


@dataclass
class SparkStreamingConfig:
    """Configuration parameters for PySpark Structured Streaming ingestion pipeline."""

    app_name: str = "MarketplaceStreamingIngestion"
    master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER_URL", "local[*]"))
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            f"localhost:{os.getenv('KAFKA_BROKER_PORT', '9092')}",
        )
    )
    schema_registry_url: str = field(
        default_factory=lambda: os.getenv(
            "SCHEMA_REGISTRY_URL",
            f"http://localhost:{os.getenv('SCHEMA_REGISTRY_PORT', '8081')}",
        )
    )
    topics: list[str] = field(default_factory=lambda: list(DEFAULT_STREAMING_TOPICS))
    dlq_topic: str = field(default_factory=lambda: os.getenv("KAFKA_DLQ_TOPIC", DEFAULT_DLQ_TOPIC))
    starting_offsets: str = field(default_factory=lambda: os.getenv("SPARK_STARTING_OFFSETS", "earliest"))
    max_offsets_per_trigger: int = field(
        default_factory=lambda: int(os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "5000"))
    )
    trigger_interval: str = field(default_factory=lambda: os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds"))
    watermark_delay: str = field(default_factory=lambda: os.getenv("SPARK_WATERMARK_DELAY", "10 minutes"))
    landing_path: str = field(
        default_factory=lambda: os.getenv("STREAMING_LANDING_PATH", "s3a://lakehouse-bronze/landing")
    )
    checkpoint_dir: str = field(
        default_factory=lambda: os.getenv(
            "STREAMING_CHECKPOINT_DIR",
            "s3a://lakehouse-bronze/checkpoints/ingestion",
        )
    )
    dlq_storage_path: str = field(default_factory=lambda: os.getenv("STREAMING_DLQ_PATH", "s3a://lakehouse-dlq/events"))
    minio_endpoint: str = field(
        default_factory=lambda: os.getenv(
            "MINIO_ENDPOINT",
            f"http://localhost:{os.getenv('MINIO_PORT', '9000')}",
        )
    )
    minio_access_key: str = field(default_factory=lambda: os.getenv("MINIO_ROOT_USER", "minioadmin"))
    minio_secret_key: str = field(default_factory=lambda: os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
    spark_packages: str = field(default_factory=lambda: os.getenv("SPARK_PACKAGES", DEFAULT_SPARK_PACKAGES))
    shuffle_partitions: int = field(default_factory=lambda: int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")))
    fail_on_data_loss: bool = False
    once: bool = False
    max_retries: int = 5
    initial_backoff_seconds: float = 2.0
    max_backoff_seconds: float = 60.0

    def __post_init__(self) -> None:
        """Validate and normalize configuration parameters."""
        # Normalize and validate topics
        if isinstance(self.topics, str):
            self.topics = [t.strip() for t in self.topics.split(",") if t.strip()]
        elif isinstance(self.topics, (list, tuple, set)):
            self.topics = [t.strip() for t in self.topics if isinstance(t, str) and t.strip()]
        else:
            raise ValueError(f"Topics must be a list of strings or comma-separated string, got {type(self.topics)}")

        if not self.topics:
            raise ValueError("Topics list must not be empty.")

        # Validate starting_offsets
        self.starting_offsets = self.starting_offsets.strip()
        if self.starting_offsets not in {"earliest", "latest"} and not self.starting_offsets.startswith("{"):
            raise ValueError(
                f"Invalid starting_offsets '{self.starting_offsets}'. "
                "Must be 'earliest', 'latest', or a JSON offset dictionary starting with '{'."
            )

        # Validate numeric bounds
        if self.max_offsets_per_trigger <= 0:
            raise ValueError(f"max_offsets_per_trigger must be > 0, got {self.max_offsets_per_trigger}")
        if self.shuffle_partitions <= 0:
            raise ValueError(f"shuffle_partitions must be > 0, got {self.shuffle_partitions}")
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")
        if self.initial_backoff_seconds <= 0:
            raise ValueError(f"initial_backoff_seconds must be > 0, got {self.initial_backoff_seconds}")
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError(
                f"max_backoff_seconds ({self.max_backoff_seconds}) must be >= "
                f"initial_backoff_seconds ({self.initial_backoff_seconds})"
            )

        # Strip trailing slashes from paths and URLs
        self.landing_path = self.landing_path.rstrip("/")
        self.checkpoint_dir = self.checkpoint_dir.rstrip("/")
        self.dlq_storage_path = self.dlq_storage_path.rstrip("/")
        self.schema_registry_url = self.schema_registry_url.rstrip("/")
        self.minio_endpoint = self.minio_endpoint.rstrip("/")

    def kafka_subscribe_pattern(self) -> str:
        """Return comma-separated string of topics for Kafka subscription.

        Returns:
            Comma-separated topic names.
        """
        return ",".join(self.topics)

    def to_kafka_read_options(self) -> dict[str, str]:
        """Convert configuration into Spark readStream kafka source options.

        Returns:
            Dictionary of Kafka read stream options.
        """
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": self.kafka_subscribe_pattern(),
            "startingOffsets": self.starting_offsets,
            "maxOffsetsPerTrigger": str(self.max_offsets_per_trigger),
            "failOnDataLoss": str(self.fail_on_data_loss).lower(),
        }

    def to_spark_conf(self) -> dict[str, str]:
        """Convert configuration into Spark and Hadoop S3A properties dictionary.

        Returns:
            Dictionary of SparkSession configuration properties.
        """
        conf: dict[str, str] = {
            "spark.app.name": self.app_name,
            "spark.master": self.master,
            "spark.jars.packages": self.spark_packages,
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.streaming.stopGracefullyOnShutdown": "true",
            "spark.hadoop.fs.s3a.endpoint": self.minio_endpoint,
            "spark.hadoop.fs.s3a.access.key": self.minio_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.minio_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.sql.session.timeZone": "UTC",
        }
        if self.minio_endpoint.startswith("http://"):
            conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = "false"
        return conf

    def get_landing_checkpoint_path(self) -> str:
        """Return checkpoint directory path for the landing stream."""
        return f"{self.checkpoint_dir}/landing"

    def get_dlq_storage_checkpoint_path(self) -> str:
        """Return checkpoint directory path for the DLQ storage stream."""
        return f"{self.checkpoint_dir}/dlq_storage"

    def get_dlq_kafka_checkpoint_path(self) -> str:
        """Return checkpoint directory path for the DLQ Kafka publisher stream."""
        return f"{self.checkpoint_dir}/dlq_kafka"


def build_streaming_arg_parser(
    description: str = "Marketplace PySpark Structured Streaming Ingestion Pipeline",
) -> argparse.ArgumentParser:
    """Build a standard ArgumentParser for the streaming ingestion pipeline.

    Args:
        description: CLI description for help messages.

    Returns:
        Configured argparse.ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        default=None,
        help="Kafka topics to consume (comma-separated or multiple values)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", f"localhost:{os.getenv('KAFKA_BROKER_PORT', '9092')}"),
        help="Kafka bootstrap broker addresses (comma-separated)",
    )
    parser.add_argument(
        "--schema-registry-url",
        type=str,
        default=os.getenv("SCHEMA_REGISTRY_URL", f"http://localhost:{os.getenv('SCHEMA_REGISTRY_PORT', '8081')}"),
        help="Confluent Schema Registry base URL",
    )
    parser.add_argument(
        "--landing-path",
        type=str,
        default=os.getenv("STREAMING_LANDING_PATH", "s3a://lakehouse-bronze/landing"),
        help="Target MinIO/S3 landing storage path for valid events",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default=os.getenv("STREAMING_CHECKPOINT_DIR", "s3a://lakehouse-bronze/checkpoints/ingestion"),
        help="Base checkpoint directory for streaming Write-Ahead Logs (WAL)",
    )
    parser.add_argument(
        "--dlq-path",
        dest="dlq_storage_path",
        type=str,
        default=os.getenv("STREAMING_DLQ_PATH", "s3a://lakehouse-dlq/events"),
        help="Target MinIO/S3 storage path for DLQ malformed events",
    )
    parser.add_argument(
        "--dlq-topic",
        type=str,
        default=os.getenv("KAFKA_DLQ_TOPIC", DEFAULT_DLQ_TOPIC),
        help="Dedicated Kafka topic for Dead Letter Queue routing",
    )
    parser.add_argument(
        "--watermark-delay",
        type=str,
        default=os.getenv("SPARK_WATERMARK_DELAY", "10 minutes"),
        help="Event-time watermark delay specification (e.g., '10 minutes', '1 hour')",
    )
    parser.add_argument(
        "--trigger-interval",
        type=str,
        default=os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds"),
        help="Micro-batch trigger interval specification (e.g., '10 seconds', '1 minute')",
    )
    parser.add_argument(
        "--starting-offsets",
        type=str,
        default=os.getenv("SPARK_STARTING_OFFSETS", "earliest"),
        help="Kafka starting offsets ('earliest', 'latest', or JSON offset string)",
    )
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=int(os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "5000")),
        help="Rate-limiting upper bound of offsets per micro-batch trigger",
    )
    parser.add_argument(
        "--master",
        type=str,
        default=os.getenv("SPARK_MASTER_URL", "local[*]"),
        help="Spark master URL (e.g., 'local[*]', 'spark://spark-master:7077')",
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="MarketplaceStreamingIngestion",
        help="Spark application name",
    )
    parser.add_argument(
        "--minio-endpoint",
        type=str,
        default=os.getenv("MINIO_ENDPOINT", f"http://localhost:{os.getenv('MINIO_PORT', '9000')}"),
        help="MinIO S3 endpoint URL",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")),
        help="Spark SQL shuffle partition count",
    )
    parser.add_argument(
        "--fail-on-data-loss",
        action="store_true",
        default=False,
        help="Fail query when Kafka data loss is detected (e.g., topic truncated)",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--once",
        action="store_true",
        dest="once",
        default=False,
        help="Execute micro-batches once (Trigger.AvailableNow) and terminate",
    )
    mode_group.add_argument(
        "--continuous",
        action="store_false",
        dest="once",
        help="Execute continuous streaming ingestion (default)",
    )

    return parser


def parse_streaming_config(
    parser: argparse.ArgumentParser | None = None,
    args: list[str] | None = None,
) -> SparkStreamingConfig:
    """Parse CLI arguments into a SparkStreamingConfig instance.

    Args:
        parser: Optional custom ArgumentParser. If None, default parser is used.
        args: Optional list of argument strings (e.g. sys.argv[1:]).

    Returns:
        Populated SparkStreamingConfig instance.
    """
    if parser is None:
        parser = build_streaming_arg_parser()

    parsed = parser.parse_args(args=args)

    topics: list[str]
    if parsed.topics:
        topics = [t.strip() for item in parsed.topics for t in item.split(",") if t.strip()]
    else:
        topics = list(DEFAULT_STREAMING_TOPICS)

    return SparkStreamingConfig(
        app_name=parsed.app_name,
        master=parsed.master,
        bootstrap_servers=parsed.bootstrap_servers,
        schema_registry_url=parsed.schema_registry_url,
        topics=topics,
        dlq_topic=parsed.dlq_topic,
        starting_offsets=parsed.starting_offsets,
        max_offsets_per_trigger=parsed.max_offsets_per_trigger,
        trigger_interval=parsed.trigger_interval,
        watermark_delay=parsed.watermark_delay,
        landing_path=parsed.landing_path,
        checkpoint_dir=parsed.checkpoint_dir,
        dlq_storage_path=parsed.dlq_storage_path,
        minio_endpoint=parsed.minio_endpoint,
        shuffle_partitions=parsed.shuffle_partitions,
        fail_on_data_loss=parsed.fail_on_data_loss,
        once=parsed.once,
    )
