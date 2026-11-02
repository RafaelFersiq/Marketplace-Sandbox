"""PySpark schema definitions and technical metadata envelope utilities for Medallion Bronze layer.

Defines the canonical technical audit metadata fields (_bronze_id, _ingested_at, _source_topic,
_kafka_partition, _kafka_offset, _schema_id, _schema_version, _payload_hash, _ingestion_batch_id,
_processing_host) and partition columns (year, month, day, hour) conforming to ADR-0008.
Provides envelope schema builders and validation functions for DataFrames and StructTypes.
"""


from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DataType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Canonical Technical Metadata Envelope Fields (ADR-0008)
BRONZE_METADATA_FIELDS: list[StructField] = [
    StructField("_bronze_id", StringType(), nullable=False),
    StructField("_ingested_at", TimestampType(), nullable=False),
    StructField("_source_topic", StringType(), nullable=False),
    StructField("_kafka_partition", IntegerType(), nullable=False),
    StructField("_kafka_offset", LongType(), nullable=False),
    StructField("_schema_id", IntegerType(), nullable=False),
    StructField("_schema_version", IntegerType(), nullable=True),
    StructField("_payload_hash", StringType(), nullable=False),
    StructField("_ingestion_batch_id", StringType(), nullable=False),
    StructField("_processing_host", StringType(), nullable=False),
]

# Canonical Partition Columns derived from event timestamp
BRONZE_PARTITION_FIELDS: list[StructField] = [
    StructField("year", IntegerType(), nullable=False),
    StructField("month", IntegerType(), nullable=False),
    StructField("day", IntegerType(), nullable=False),
    StructField("hour", IntegerType(), nullable=True),
]

# Column name index constants
BRONZE_METADATA_COLUMN_NAMES: list[str] = [field.name for field in BRONZE_METADATA_FIELDS]
BRONZE_PARTITION_COLUMN_NAMES: list[str] = [field.name for field in BRONZE_PARTITION_FIELDS]
BRONZE_ALL_TECHNICAL_COLUMNS: list[str] = (
    BRONZE_METADATA_COLUMN_NAMES + BRONZE_PARTITION_COLUMN_NAMES
)

# Required metadata fields that must never be omitted
BRONZE_REQUIRED_METADATA_COLUMNS: frozenset[str] = frozenset(
    {
        "_bronze_id",
        "_ingested_at",
        "_source_topic",
        "_kafka_partition",
        "_kafka_offset",
        "_schema_id",
        "_payload_hash",
        "_ingestion_batch_id",
        "_processing_host",
    }
)

# Expected data types dictionary for fast type matching
_EXPECTED_TECHNICAL_DATA_TYPES: dict[str, type[DataType]] = {
    field.name: type(field.dataType)
    for field in (BRONZE_METADATA_FIELDS + BRONZE_PARTITION_FIELDS)
}


class BronzeSchemaValidationError(ValueError):
    """Raised when a DataFrame or StructType violates Medallion Bronze metadata contract."""

    def __init__(self, message: str, missing_fields: list[str] | None = None) -> None:
        """Initialize BronzeSchemaValidationError with detailed diagnostic message.

        Args:
            message: Descriptive explanation of validation failure.
            missing_fields: Optional list of missing field names.
        """
        super().__init__(message)
        self.missing_fields = missing_fields or []


def get_bronze_metadata_schema() -> StructType:
    """Return the PySpark StructType containing the 10 canonical technical metadata envelope fields.

    Returns:
        PySpark StructType definition.
    """
    return StructType(list(BRONZE_METADATA_FIELDS))


def get_bronze_partition_schema(include_hour: bool = True) -> StructType:
    """Return the PySpark StructType containing canonical time-based partition columns.

    Args:
        include_hour: Whether to include 'hour' column in partition schema.

    Returns:
        PySpark StructType containing year, month, day (and optionally hour).
    """
    if include_hour:
        return StructType(list(BRONZE_PARTITION_FIELDS))
    return StructType([f for f in BRONZE_PARTITION_FIELDS if f.name != "hour"])


def build_bronze_enveloped_schema(
    payload_schema: StructType,
    include_partition_cols: bool = True,
    include_hour: bool = True,
) -> StructType:
    """Construct a complete Bronze enveloped StructType merging domain payload schema with technical metadata.

    Preserves original domain payload fields non-destructively, appends technical audit metadata
    envelope columns, and appends time-based partition columns without field duplication.

    Args:
        payload_schema: Raw domain payload schema (Avro/JSON decoded structure).
        include_partition_cols: Whether to append time partition columns (year, month, day, hour).
        include_hour: Whether to include hour in partition columns.

    Returns:
        Consolidated StructType containing payload + metadata envelope + partition fields.
    """
    fields: list[StructField] = []
    seen_names: set[str] = set()

    # 1. Add domain payload fields
    for field in payload_schema.fields:
        if field.name not in seen_names:
            fields.append(field)
            seen_names.add(field.name)

    # 2. Add technical metadata envelope fields (preserve metadata definition if name clashes)
    for field in BRONZE_METADATA_FIELDS:
        if field.name in seen_names:
            fields = [f for f in fields if f.name != field.name]
        fields.append(field)
        seen_names.add(field.name)

    # 3. Add partition columns
    if include_partition_cols:
        partition_fields = (
            BRONZE_PARTITION_FIELDS
            if include_hour
            else [f for f in BRONZE_PARTITION_FIELDS if f.name != "hour"]
        )
        for field in partition_fields:
            if field.name in seen_names:
                fields = [f for f in fields if f.name != field.name]
            fields.append(field)
            seen_names.add(field.name)

    return StructType(fields)


def validate_bronze_schema(
    schema_or_df: StructType | DataFrame,
    require_partition_columns: bool = False,
    check_data_types: bool = True,
) -> tuple[bool, list[str]]:
    """Validate whether a DataFrame or StructType complies with Bronze technical metadata specification.

    Checks presence of mandatory technical metadata envelope columns and optional partition columns,
    and validates data type compatibility against ADR-0008 specification.

    Args:
        schema_or_df: PySpark DataFrame or StructType to validate.
        require_partition_columns: If True, checks that 'year', 'month', and 'day' are present.
        check_data_types: If True, validates data type compatibility of technical columns.

    Returns:
        Tuple of (is_valid: bool, error_messages: list[str]).
    """
    schema: StructType
    if isinstance(schema_or_df, DataFrame):
        schema = schema_or_df.schema
    elif isinstance(schema_or_df, StructType):
        schema = schema_or_df
    else:
        return False, [f"Expected DataFrame or StructType, got {type(schema_or_df).__name__}"]

    field_map: dict[str, StructField] = {f.name: f for f in schema.fields}
    errors: list[str] = []

    # 1. Validate mandatory technical metadata columns
    for required_col in BRONZE_REQUIRED_METADATA_COLUMNS:
        if required_col not in field_map:
            errors.append(f"Missing mandatory Bronze technical metadata field: '{required_col}'")

    # 2. Validate partition columns if required
    if require_partition_columns:
        for part_col in ("year", "month", "day"):
            if part_col not in field_map:
                errors.append(f"Missing required partition column: '{part_col}'")

    # 3. Validate column data types
    if check_data_types:
        for col_name, field in field_map.items():
            if col_name in _EXPECTED_TECHNICAL_DATA_TYPES:
                expected_type_cls = _EXPECTED_TECHNICAL_DATA_TYPES[col_name]
                actual_type_cls = type(field.dataType)
                if actual_type_cls is not expected_type_cls:
                    errors.append(
                        f"Field '{col_name}' has incorrect type '{actual_type_cls.__name__}'; "
                        f"expected '{expected_type_cls.__name__}'"
                    )

    return len(errors) == 0, errors


def validate_bronze_schema_or_raise(
    schema_or_df: StructType | DataFrame,
    require_partition_columns: bool = False,
    check_data_types: bool = True,
) -> None:
    """Validate Bronze metadata compliance and raise BronzeSchemaValidationError on failure.

    Args:
        schema_or_df: PySpark DataFrame or StructType to validate.
        require_partition_columns: If True, checks that 'year', 'month', and 'day' are present.
        check_data_types: If True, validates data type compatibility of technical columns.

    Raises:
        BronzeSchemaValidationError: If one or more required fields are missing or typed incorrectly.
    """
    is_valid, errors = validate_bronze_schema(
        schema_or_df,
        require_partition_columns=require_partition_columns,
        check_data_types=check_data_types,
    )
    if not is_valid:
        missing = [
            e.split("'")[1]
            for e in errors
            if "Missing mandatory" in e or "Missing required" in e
        ]
        msg = f"Bronze schema contract validation failed ({len(errors)} error(s)):\n - " + "\n - ".join(errors)
        raise BronzeSchemaValidationError(message=msg, missing_fields=missing)
