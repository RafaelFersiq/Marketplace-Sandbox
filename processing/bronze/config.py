"""Medallion Bronze layer storage configuration and CLI argument parsing utilities.

Defines BronzeStorageConfig dataclass and helper routines to configure landing and canonical
Bronze storage paths, partition schemes, compression formats, target compaction file sizes,
MinIO/S3A credentials, PostgreSQL metadata catalog settings, and lifecycle TTL parameters.
"""

import argparse
import os
from dataclasses import dataclass, field
from typing import Any

# Default topics across all 6 business domains
DEFAULT_TOPICS: tuple[str, ...] = (
    "marketplace.orders",
    "marketplace.payments",
    "marketplace.cart",
    "marketplace.inventory",
    "marketplace.catalog",
    "marketplace.navigation",
)

# Standard partition keys for canonical Bronze storage layout
DEFAULT_BRONZE_PARTITION_KEYS: tuple[str, ...] = ("topic", "year", "month", "day")

# Default storage paths in MinIO / S3
DEFAULT_BRONZE_LANDING_PATH: str = "s3a://lakehouse-bronze/landing"
DEFAULT_BRONZE_ROOT_PATH: str = "s3a://lakehouse-bronze/events"
DEFAULT_BRONZE_CHECKPOINT_DIR: str = "s3a://lakehouse-bronze/checkpoints/bronze"

# Storage optimization defaults
DEFAULT_BRONZE_COMPRESSION: str = "snappy"
DEFAULT_BRONZE_COMPACTION_FILE_SIZE_MB: int = 128
DEFAULT_BRONZE_COMPACTION_THRESHOLD_FILES: int = 10
DEFAULT_BRONZE_COMPACTION_MIN_FILE_SIZE_MB: int = 32

# Lifecycle & Retention defaults
DEFAULT_BRONZE_LANDING_TTL_HOURS: int = 48
DEFAULT_BRONZE_AUDIT_RETENTION_DAYS: int = 90

# PostgreSQL Metadata Catalog defaults
DEFAULT_METADATA_SCHEMA: str = "lakehouse_metadata"
DEFAULT_BRONZE_PARTITION_TABLE: str = "bronze_partition_catalog"
DEFAULT_BRONZE_AUDIT_TABLE: str = "bronze_ingestion_audit"

# Spark default packages
DEFAULT_SPARK_PACKAGES: str = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
    "org.apache.spark:spark-avro_2.12:3.5.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.7.2"
)

# Allowed columnar compression codecs
ALLOWED_COMPRESSION_CODECS: frozenset[str] = frozenset(
    {"snappy", "gzip", "zstd", "lz4", "none", "uncompressed"}
)


@dataclass
class BronzeStorageConfig:
    """Configuration parameters for Medallion Bronze storage, partitioning, compaction, and cataloging."""

    app_name: str = "MarketplaceBronzeProcessing"
    master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER_URL", "local[*]"))
    landing_path: str = field(
        default_factory=lambda: os.getenv(
            "BRONZE_LANDING_PATH",
            os.getenv("STREAMING_LANDING_PATH", DEFAULT_BRONZE_LANDING_PATH),
        )
    )
    bronze_root_path: str = field(
        default_factory=lambda: os.getenv(
            "BRONZE_ROOT_PATH",
            os.getenv("BRONZE_STORAGE_PATH", DEFAULT_BRONZE_ROOT_PATH),
        )
    )
    checkpoint_dir: str = field(
        default_factory=lambda: os.getenv("BRONZE_CHECKPOINT_DIR", DEFAULT_BRONZE_CHECKPOINT_DIR)
    )
    partition_keys: list[str] = field(default_factory=lambda: list(DEFAULT_BRONZE_PARTITION_KEYS))
    topics: list[str] = field(default_factory=lambda: list(DEFAULT_TOPICS))
    compression: str = field(
        default_factory=lambda: os.getenv("BRONZE_COMPRESSION", DEFAULT_BRONZE_COMPRESSION)
    )
    target_file_size_mb: int = field(
        default_factory=lambda: int(
            os.getenv("BRONZE_TARGET_FILE_SIZE_MB", str(DEFAULT_BRONZE_COMPACTION_FILE_SIZE_MB))
        )
    )
    compaction_threshold_file_count: int = field(
        default_factory=lambda: int(
            os.getenv(
                "BRONZE_COMPACTION_THRESHOLD_FILES",
                str(DEFAULT_BRONZE_COMPACTION_THRESHOLD_FILES),
            )
        )
    )
    compaction_min_file_size_mb: int = field(
        default_factory=lambda: int(
            os.getenv(
                "BRONZE_COMPACTION_MIN_FILE_SIZE_MB",
                str(DEFAULT_BRONZE_COMPACTION_MIN_FILE_SIZE_MB),
            )
        )
    )
    landing_ttl_hours: int = field(
        default_factory=lambda: int(
            os.getenv("BRONZE_LANDING_TTL_HOURS", str(DEFAULT_BRONZE_LANDING_TTL_HOURS))
        )
    )
    audit_retention_days: int = field(
        default_factory=lambda: int(
            os.getenv("BRONZE_AUDIT_RETENTION_DAYS", str(DEFAULT_BRONZE_AUDIT_RETENTION_DAYS))
        )
    )

    # MinIO / S3 Connection Properties
    minio_endpoint: str = field(
        default_factory=lambda: os.getenv(
            "MINIO_ENDPOINT",
            f"http://localhost:{os.getenv('MINIO_PORT', '9000')}",
        )
    )
    minio_access_key: str = field(
        default_factory=lambda: os.getenv("MINIO_ROOT_USER", "minioadmin")
    )
    minio_secret_key: str = field(
        default_factory=lambda: os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    )

    # PostgreSQL Metadata Catalog Connection Properties
    postgres_host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    postgres_port: int = field(
        default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432"))
    )
    postgres_db: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "marketplace"))
    postgres_user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "postgres"))
    postgres_password: str = field(
        default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "postgres")
    )
    postgres_metadata_schema: str = field(
        default_factory=lambda: os.getenv("POSTGRES_METADATA_SCHEMA", DEFAULT_METADATA_SCHEMA)
    )
    postgres_partition_table: str = field(
        default_factory=lambda: os.getenv("BRONZE_PARTITION_TABLE", DEFAULT_BRONZE_PARTITION_TABLE)
    )
    postgres_audit_table: str = field(
        default_factory=lambda: os.getenv("BRONZE_AUDIT_TABLE", DEFAULT_BRONZE_AUDIT_TABLE)
    )

    # Spark Runtime Properties
    spark_packages: str = field(
        default_factory=lambda: os.getenv("SPARK_PACKAGES", DEFAULT_SPARK_PACKAGES)
    )
    shuffle_partitions: int = field(
        default_factory=lambda: int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
    )
    dry_run: bool = False

    def __post_init__(self) -> None:
        """Validate, normalize, and sanity-check configuration parameters."""
        # Normalize and validate topics
        if isinstance(self.topics, str):
            self.topics = [t.strip() for t in self.topics.split(",") if t.strip()]
        elif isinstance(self.topics, (list, tuple, set)):
            self.topics = [t.strip() for t in self.topics if isinstance(t, str) and t.strip()]
        else:
            raise ValueError(
                f"Topics must be a list of strings or comma-separated string, got {type(self.topics)}"
            )

        if not self.topics:
            raise ValueError("Topics list must not be empty.")

        # Normalize and validate partition keys
        if isinstance(self.partition_keys, str):
            self.partition_keys = [
                k.strip() for k in self.partition_keys.split(",") if k.strip()
            ]
        elif isinstance(self.partition_keys, (list, tuple, set)):
            self.partition_keys = [
                k.strip() for k in self.partition_keys if isinstance(k, str) and k.strip()
            ]
        else:
            raise ValueError(
                f"Partition keys must be a list of strings, got {type(self.partition_keys)}"
            )

        if not self.partition_keys:
            raise ValueError("Partition keys list must not be empty.")

        # Validate compression codec
        self.compression = self.compression.lower().strip()
        if self.compression not in ALLOWED_COMPRESSION_CODECS:
            raise ValueError(
                f"Invalid compression codec '{self.compression}'. "
                f"Supported codecs: {sorted(ALLOWED_COMPRESSION_CODECS)}"
            )

        # Validate numeric bounds
        if self.target_file_size_mb <= 0:
            raise ValueError(f"target_file_size_mb must be > 0, got {self.target_file_size_mb}")
        if self.compaction_threshold_file_count <= 0:
            raise ValueError(
                f"compaction_threshold_file_count must be > 0, got {self.compaction_threshold_file_count}"
            )
        if self.compaction_min_file_size_mb <= 0:
            raise ValueError(
                f"compaction_min_file_size_mb must be > 0, got {self.compaction_min_file_size_mb}"
            )
        if self.landing_ttl_hours < 0:
            raise ValueError(f"landing_ttl_hours must be >= 0, got {self.landing_ttl_hours}")
        if self.audit_retention_days < 0:
            raise ValueError(
                f"audit_retention_days must be >= 0, got {self.audit_retention_days}"
            )
        if not (1 <= self.postgres_port <= 65535):
            raise ValueError(f"postgres_port must be between 1 and 65535, got {self.postgres_port}")
        if self.shuffle_partitions <= 0:
            raise ValueError(f"shuffle_partitions must be > 0, got {self.shuffle_partitions}")

        # Strip trailing slashes from paths and endpoints
        self.landing_path = self.landing_path.rstrip("/")
        self.bronze_root_path = self.bronze_root_path.rstrip("/")
        self.checkpoint_dir = self.checkpoint_dir.rstrip("/")
        self.minio_endpoint = self.minio_endpoint.rstrip("/")

    @property
    def target_file_size_bytes(self) -> int:
        """Return the target compaction file size in bytes."""
        return self.target_file_size_mb * 1024 * 1024

    @property
    def compaction_min_file_size_bytes(self) -> int:
        """Return the minimum threshold file size for small-file compaction in bytes."""
        return self.compaction_min_file_size_mb * 1024 * 1024

    @property
    def postgres_jdbc_url(self) -> str:
        """Return PostgreSQL JDBC connection URL for Spark JDBC operations."""
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def postgres_connection_dict(self) -> dict[str, Any]:
        """Return connection parameter dictionary for psycopg/psycopg2/asyncpg client libraries."""
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "dbname": self.postgres_db,
            "user": self.postgres_user,
            "password": self.postgres_password,
        }

    @property
    def postgres_jdbc_properties(self) -> dict[str, str]:
        """Return standard connection properties dictionary for Spark read/write JDBC calls."""
        return {
            "user": self.postgres_user,
            "password": self.postgres_password,
            "driver": "org.postgresql.Driver",
        }

    @property
    def full_partition_table_name(self) -> str:
        """Return fully-qualified schema.table identifier for bronze partition catalog."""
        return f"{self.postgres_metadata_schema}.{self.postgres_partition_table}"

    @property
    def full_audit_table_name(self) -> str:
        """Return fully-qualified schema.table identifier for bronze ingestion audit table."""
        return f"{self.postgres_metadata_schema}.{self.postgres_audit_table}"

    def get_topic_root_path(self, topic: str) -> str:
        """Return the root Bronze storage path for a given topic.

        Args:
            topic: Kafka topic / business domain identifier.

        Returns:
            URI string: s3a://lakehouse-bronze/events/{topic}
        """
        return f"{self.bronze_root_path}/{topic}"

    def get_topic_partition_path(self, topic: str, year: int, month: int, day: int) -> str:
        """Return the canonical partition directory URI for a given topic and date.

        Args:
            topic: Kafka topic / business domain identifier.
            year: Partition year (YYYY).
            month: Partition month (1-12).
            day: Partition day (1-31).

        Returns:
            URI string: s3a://lakehouse-bronze/events/{topic}/year={YYYY}/month={MM}/day={DD}
        """
        return f"{self.bronze_root_path}/{topic}/year={year:04d}/month={month:02d}/day={day:02d}"

    def get_compaction_staging_path(
        self, topic: str, year: int, month: int, day: int, batch_id: str
    ) -> str:
        """Return the temporary staging directory URI for atomic compaction swaps.

        Args:
            topic: Kafka topic / business domain identifier.
            year: Partition year (YYYY).
            month: Partition month (1-12).
            day: Partition day (1-31).
            batch_id: Compaction batch run identifier.

        Returns:
            URI string: s3a://.../events/{topic}/year=.../month=.../day=.../_compacting_staging_{batch_id}
        """
        partition_path = self.get_topic_partition_path(topic, year, month, day)
        return f"{partition_path}/_compacting_staging_{batch_id}"

    def to_spark_conf(self) -> dict[str, str]:
        """Convert configuration into Spark and Hadoop S3A properties dictionary.

        Returns:
            Dictionary of SparkSession configuration options.
        """
        conf: dict[str, str] = {
            "spark.app.name": self.app_name,
            "spark.master": self.master,
            "spark.jars.packages": self.spark_packages,
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.parquet.compression.codec": self.compression,
            "spark.sql.parquet.mergeSchema": "false",
            "spark.hadoop.fs.s3a.endpoint": self.minio_endpoint,
            "spark.hadoop.fs.s3a.access.key": self.minio_access_key,
            "spark.hadoop.fs.s3a.secret.key": self.minio_secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            ),
        }
        if self.minio_endpoint.startswith("http://"):
            conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = "false"
        return conf


def build_bronze_arg_parser(
    description: str = "Marketplace Bronze Layer Ingestion & Compaction Engine",
) -> argparse.ArgumentParser:
    """Build a standard ArgumentParser for Bronze ingestion and compaction jobs.

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
        help="Topics to process (comma-separated or multiple values)",
    )
    parser.add_argument(
        "--landing-path",
        type=str,
        default=os.getenv(
            "BRONZE_LANDING_PATH",
            os.getenv("STREAMING_LANDING_PATH", DEFAULT_BRONZE_LANDING_PATH),
        ),
        help="Source landing zone directory containing raw micro-batches",
    )
    parser.add_argument(
        "--bronze-path",
        dest="bronze_root_path",
        type=str,
        default=os.getenv(
            "BRONZE_ROOT_PATH",
            os.getenv("BRONZE_STORAGE_PATH", DEFAULT_BRONZE_ROOT_PATH),
        ),
        help="Canonical Bronze storage root directory for partitioned Parquet datasets",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default=os.getenv("BRONZE_CHECKPOINT_DIR", DEFAULT_BRONZE_CHECKPOINT_DIR),
        help="Base checkpoint directory for Bronze promotion WAL",
    )
    parser.add_argument(
        "--compression",
        type=str,
        choices=sorted(ALLOWED_COMPRESSION_CODECS),
        default=os.getenv("BRONZE_COMPRESSION", DEFAULT_BRONZE_COMPRESSION),
        help="Columnar Parquet compression codec",
    )
    parser.add_argument(
        "--target-file-size-mb",
        type=int,
        default=int(
            os.getenv("BRONZE_TARGET_FILE_SIZE_MB", str(DEFAULT_BRONZE_COMPACTION_FILE_SIZE_MB))
        ),
        help="Target Parquet file size in megabytes for small-file compaction",
    )
    parser.add_argument(
        "--compaction-threshold-files",
        dest="compaction_threshold_file_count",
        type=int,
        default=int(
            os.getenv(
                "BRONZE_COMPACTION_THRESHOLD_FILES",
                str(DEFAULT_BRONZE_COMPACTION_THRESHOLD_FILES),
            )
        ),
        help="Number of files in a partition triggering compaction",
    )
    parser.add_argument(
        "--landing-ttl-hours",
        type=int,
        default=int(
            os.getenv("BRONZE_LANDING_TTL_HOURS", str(DEFAULT_BRONZE_LANDING_TTL_HOURS))
        ),
        help="Time-to-live in hours before landed micro-batches are eligible for pruning",
    )
    parser.add_argument(
        "--minio-endpoint",
        type=str,
        default=os.getenv(
            "MINIO_ENDPOINT",
            f"http://localhost:{os.getenv('MINIO_PORT', '9000')}",
        ),
        help="MinIO S3 endpoint URL",
    )
    parser.add_argument(
        "--postgres-host",
        type=str,
        default=os.getenv("POSTGRES_HOST", "localhost"),
        help="PostgreSQL catalog hostname",
    )
    parser.add_argument(
        "--postgres-port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", "5432")),
        help="PostgreSQL catalog port",
    )
    parser.add_argument(
        "--postgres-db",
        type=str,
        default=os.getenv("POSTGRES_DB", "marketplace"),
        help="PostgreSQL database name",
    )
    parser.add_argument(
        "--master",
        type=str,
        default=os.getenv("SPARK_MASTER_URL", "local[*]"),
        help="Spark master URL (e.g. 'local[*]', 'spark://spark-master:7077')",
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="MarketplaceBronzeProcessing",
        help="Spark application name",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")),
        help="Spark SQL shuffle partition count",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Execute plan without committing writes to Bronze storage or PostgreSQL catalog",
    )
    return parser


def parse_bronze_config(
    parser: argparse.ArgumentParser | None = None,
    args: list[str] | None = None,
) -> BronzeStorageConfig:
    """Parse CLI arguments into a BronzeStorageConfig instance.

    Args:
        parser: Optional custom ArgumentParser. If None, default parser is used.
        args: Optional list of argument strings (e.g. sys.argv[1:]).

    Returns:
        Populated BronzeStorageConfig instance.
    """
    if parser is None:
        parser = build_bronze_arg_parser()

    parsed = parser.parse_args(args=args)

    topics: list[str]
    if parsed.topics:
        topics = [t.strip() for item in parsed.topics for t in item.split(",") if t.strip()]
    else:
        topics = list(DEFAULT_TOPICS)

    return BronzeStorageConfig(
        app_name=parsed.app_name,
        master=parsed.master,
        landing_path=parsed.landing_path,
        bronze_root_path=parsed.bronze_root_path,
        checkpoint_dir=parsed.checkpoint_dir,
        topics=topics,
        compression=parsed.compression,
        target_file_size_mb=parsed.target_file_size_mb,
        compaction_threshold_file_count=parsed.compaction_threshold_file_count,
        landing_ttl_hours=parsed.landing_ttl_hours,
        minio_endpoint=parsed.minio_endpoint,
        postgres_host=parsed.postgres_host,
        postgres_port=parsed.postgres_port,
        postgres_db=parsed.postgres_db,
        shuffle_partitions=parsed.shuffle_partitions,
        dry_run=parsed.dry_run,
    )
