"""Medallion Bronze layer processing package for Marketplace Sandbox.

Provides Bronze storage configuration, technical metadata schema definitions,
envelope validation utilities, partition management, compaction settings,
technical metadata enrichment transformers, atomic Parquet table writers,
metadata catalog management, small-file compactor, raw landing retention lifecycle,
and Prometheus telemetry instrumentation.
"""

from processing.bronze.catalog import (
    BronzeAuditCatalogError,
    BronzeCatalogError,
    BronzeCatalogManager,
    BronzeConnectionError,
    BronzePartitionCatalogError,
)
from processing.bronze.compaction import (
    BronzeCompactionResult,
    BronzeCompactor,
)
from processing.bronze.config import (
    ALLOWED_COMPRESSION_CODECS,
    DEFAULT_BRONZE_AUDIT_RETENTION_DAYS,
    DEFAULT_BRONZE_AUDIT_TABLE,
    DEFAULT_BRONZE_CHECKPOINT_DIR,
    DEFAULT_BRONZE_COMPACTION_FILE_SIZE_MB,
    DEFAULT_BRONZE_COMPACTION_MIN_FILE_SIZE_MB,
    DEFAULT_BRONZE_COMPACTION_THRESHOLD_FILES,
    DEFAULT_BRONZE_COMPRESSION,
    DEFAULT_BRONZE_LANDING_PATH,
    DEFAULT_BRONZE_LANDING_TTL_HOURS,
    DEFAULT_BRONZE_PARTITION_KEYS,
    DEFAULT_BRONZE_PARTITION_TABLE,
    DEFAULT_BRONZE_ROOT_PATH,
    DEFAULT_METADATA_SCHEMA,
    DEFAULT_TOPICS,
    BronzeStorageConfig,
    build_bronze_arg_parser,
    parse_bronze_config,
)
from processing.bronze.enrichment import (
    BronzeMetadataEnricher,
    enrich_with_bronze_envelope,
)
from processing.bronze.metrics import (
    BronzeMetricSnapshot,
    BronzeMetricsRegistry,
    get_bronze_metrics_registry,
    reset_bronze_metrics_registry,
)
from processing.bronze.reader import (
    BronzeDatasetReader,
    BronzeReaderError,
    BronzeSparkInitError,
)
from processing.bronze.retention import (
    BronzeRetentionManager,
    RetentionPruneResult,
)
from processing.bronze.schema import (
    BRONZE_ALL_TECHNICAL_COLUMNS,
    BRONZE_METADATA_COLUMN_NAMES,
    BRONZE_METADATA_FIELDS,
    BRONZE_PARTITION_COLUMN_NAMES,
    BRONZE_PARTITION_FIELDS,
    BRONZE_REQUIRED_METADATA_COLUMNS,
    BronzeSchemaValidationError,
    build_bronze_enveloped_schema,
    get_bronze_metadata_schema,
    get_bronze_partition_schema,
    validate_bronze_schema,
    validate_bronze_schema_or_raise,
)
from processing.bronze.writer import (
    BronzeTableWriter,
    BronzeWriteResult,
)

__all__ = [
    "ALLOWED_COMPRESSION_CODECS",
    "BRONZE_ALL_TECHNICAL_COLUMNS",
    "BRONZE_METADATA_COLUMN_NAMES",
    "BRONZE_METADATA_FIELDS",
    "BRONZE_PARTITION_COLUMN_NAMES",
    "BRONZE_PARTITION_FIELDS",
    "BRONZE_REQUIRED_METADATA_COLUMNS",
    "DEFAULT_BRONZE_AUDIT_RETENTION_DAYS",
    "DEFAULT_BRONZE_AUDIT_TABLE",
    "DEFAULT_BRONZE_CHECKPOINT_DIR",
    "DEFAULT_BRONZE_COMPACTION_FILE_SIZE_MB",
    "DEFAULT_BRONZE_COMPACTION_MIN_FILE_SIZE_MB",
    "DEFAULT_BRONZE_COMPACTION_THRESHOLD_FILES",
    "DEFAULT_BRONZE_COMPRESSION",
    "DEFAULT_BRONZE_LANDING_PATH",
    "DEFAULT_BRONZE_LANDING_TTL_HOURS",
    "DEFAULT_BRONZE_PARTITION_KEYS",
    "DEFAULT_BRONZE_PARTITION_TABLE",
    "DEFAULT_BRONZE_ROOT_PATH",
    "DEFAULT_METADATA_SCHEMA",
    "DEFAULT_TOPICS",
    "BronzeAuditCatalogError",
    "BronzeCatalogError",
    "BronzeCatalogManager",
    "BronzeCompactionResult",
    "BronzeCompactor",
    "BronzeConnectionError",
    "BronzeDatasetReader",
    "BronzeMetadataEnricher",
    "BronzeMetricSnapshot",
    "BronzeMetricsRegistry",
    "BronzePartitionCatalogError",
    "BronzeReaderError",
    "BronzeRetentionManager",
    "BronzeSchemaValidationError",
    "BronzeSparkInitError",
    "BronzeStorageConfig",
    "BronzeTableWriter",
    "BronzeWriteResult",
    "RetentionPruneResult",
    "build_bronze_arg_parser",
    "build_bronze_enveloped_schema",
    "enrich_with_bronze_envelope",
    "get_bronze_metadata_schema",
    "get_bronze_metrics_registry",
    "get_bronze_partition_schema",
    "parse_bronze_config",
    "reset_bronze_metrics_registry",
    "validate_bronze_schema",
    "validate_bronze_schema_or_raise",
]
