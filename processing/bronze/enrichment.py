"""PySpark technical metadata enrichment and event-time partition deriver for Medallion Bronze.

Enriches incoming micro-batch DataFrames with the 10 canonical technical metadata envelope fields
(_bronze_id, _ingested_at, _source_topic, _kafka_partition, _kafka_offset, _schema_id, _schema_version,
_payload_hash, _ingestion_batch_id, _processing_host) and derives time-based partition columns
(year, month, day, hour) from event timestamps with fallback to ingestion time conforming to ADR-0008.
Supports non-destructive domain payload extraction and validates contract compliance.
"""

from __future__ import annotations

import logging
import socket
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

from processing.bronze.schema import (
    validate_bronze_schema_or_raise,
)

logger = logging.getLogger("processing.bronze.enrichment")

# Deterministic SHA-256 digest of empty string as fallback
EMPTY_SHA256_HEX = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


class BronzeMetadataEnricher:
    """Enriches DataFrames with canonical Bronze technical metadata envelope and partition keys."""

    def __init__(
        self,
        batch_id: str | None = None,
        topic: str | None = None,
        processing_host: str | None = None,
        schema_version_lookup: dict[int, int] | None = None,
        domain_schema: StructType | None = None,
        unpack_payload: bool = False,
        include_hour_partition: bool = True,
        validate_contract: bool = True,
    ) -> None:
        """Initialize BronzeMetadataEnricher transformer.

        Args:
            batch_id: Optional default batch ID (UUID generated if omitted).
            topic: Optional default source topic name.
            processing_host: Optional hostname/worker identifier running the job.
            schema_version_lookup: Optional mapping of Confluent schema_id -> schema_version.
            domain_schema: Optional PySpark StructType for unpacking domain payload fields.
            unpack_payload: Whether to unpack domain fields from payload_json non-destructively.
            include_hour_partition: Whether to include 'hour' column in partition set.
            validate_contract: Whether to enforce validate_bronze_schema_or_raise() post-enrichment.
        """
        self.batch_id = batch_id
        self.topic = topic
        self.processing_host = processing_host or socket.gethostname()
        self.schema_version_lookup = schema_version_lookup or {}
        self.domain_schema = domain_schema
        self.unpack_payload = unpack_payload
        self.include_hour_partition = include_hour_partition
        self.validate_contract = validate_contract

    def enrich(
        self,
        df: DataFrame,
        batch_id: str | None = None,
        topic: str | None = None,
        processing_host: str | None = None,
        schema_version_lookup: dict[int, int] | None = None,
    ) -> DataFrame:
        """Enrich DataFrame with 10 technical metadata columns and partition columns.

        Args:
            df: Input PySpark DataFrame (landing micro-batch or batch dataset).
            batch_id: Execution batch UUID override.
            topic: Topic override.
            processing_host: Hostname override.
            schema_version_lookup: Schema version dictionary override.

        Returns:
            Enriched PySpark DataFrame compliant with Bronze technical metadata contract.
        """
        effective_batch_id = batch_id or self.batch_id or str(uuid.uuid4())
        effective_topic = topic or self.topic
        effective_host = processing_host or self.processing_host
        lookup_map = schema_version_lookup if schema_version_lookup is not None else self.schema_version_lookup

        logger.info(
            "Starting Bronze technical metadata enrichment",
            extra={
                "batch_id": effective_batch_id,
                "topic": effective_topic,
                "host": effective_host,
            },
        )

        # 1. _bronze_id (UUIDv4)
        if "_bronze_id" not in df.columns:
            df = df.withColumn("_bronze_id", F.expr("uuid()"))
        else:
            df = df.withColumn("_bronze_id", F.col("_bronze_id").cast(StringType()))

        # 2. _ingested_at (Current UTC timestamp)
        if "_ingested_at" not in df.columns:
            df = df.withColumn("_ingested_at", F.current_timestamp())
        else:
            df = df.withColumn("_ingested_at", F.col("_ingested_at").cast(TimestampType()))

        # 3. _source_topic
        if effective_topic is not None:
            df = df.withColumn("_source_topic", F.lit(effective_topic).cast(StringType()))
        elif "_source_topic" in df.columns:
            df = df.withColumn("_source_topic", F.col("_source_topic").cast(StringType()))
        elif "topic" in df.columns:
            df = df.withColumn("_source_topic", F.col("topic").cast(StringType()))
        else:
            df = df.withColumn("_source_topic", F.lit("unknown").cast(StringType()))

        # 4. _kafka_partition
        if "_kafka_partition" in df.columns:
            df = df.withColumn("_kafka_partition", F.col("_kafka_partition").cast(IntegerType()))
        elif "partition" in df.columns:
            df = df.withColumn("_kafka_partition", F.col("partition").cast(IntegerType()))
        else:
            df = df.withColumn("_kafka_partition", F.lit(0).cast(IntegerType()))

        # 5. _kafka_offset
        if "_kafka_offset" in df.columns:
            df = df.withColumn("_kafka_offset", F.col("_kafka_offset").cast(LongType()))
        elif "offset" in df.columns:
            df = df.withColumn("_kafka_offset", F.col("offset").cast(LongType()))
        else:
            df = df.withColumn("_kafka_offset", F.lit(0).cast(LongType()))

        # 6. _schema_id
        if "_schema_id" in df.columns:
            df = df.withColumn("_schema_id", F.col("_schema_id").cast(IntegerType()))
        elif "schema_id" in df.columns:
            df = df.withColumn("_schema_id", F.col("schema_id").cast(IntegerType()))
        else:
            df = df.withColumn("_schema_id", F.lit(0).cast(IntegerType()))

        # 7. _schema_version
        version_lookup_expr = F.lit(None).cast(IntegerType())
        if lookup_map:
            for sid, sver in lookup_map.items():
                version_lookup_expr = F.when(
                    F.col("_schema_id") == F.lit(int(sid)),
                    F.lit(int(sver)).cast(IntegerType()),
                ).otherwise(version_lookup_expr)

        if "_schema_version" in df.columns:
            df = df.withColumn(
                "_schema_version",
                F.coalesce(F.col("_schema_version").cast(IntegerType()), version_lookup_expr),
            )
        elif "schema_version" in df.columns:
            df = df.withColumn(
                "_schema_version",
                F.coalesce(F.col("schema_version").cast(IntegerType()), version_lookup_expr),
            )
        else:
            df = df.withColumn("_schema_version", version_lookup_expr)

        # 8. _payload_hash (Deterministic SHA-256)
        if "payload_json" in df.columns:
            raw_hash = F.sha2(F.col("payload_json").cast(StringType()), 256)
        elif "value" in df.columns:
            raw_hash = F.sha2(F.col("value"), 256)
        elif "_payload_hash" in df.columns:
            raw_hash = F.col("_payload_hash").cast(StringType())
        else:
            raw_hash = F.lit(EMPTY_SHA256_HEX)

        df = df.withColumn(
            "_payload_hash",
            F.coalesce(raw_hash, F.lit(EMPTY_SHA256_HEX)).cast(StringType()),
        )

        # 9. _ingestion_batch_id
        df = df.withColumn("_ingestion_batch_id", F.lit(effective_batch_id).cast(StringType()))

        # 10. _processing_host
        df = df.withColumn("_processing_host", F.lit(effective_host).cast(StringType()))

        # Partition Derivation: year, month, day, hour
        if "event_time" in df.columns:
            event_ts_col = F.to_timestamp(F.col("event_time"))
        elif "event_timestamp_ms" in df.columns:
            event_ts_col = (F.col("event_timestamp_ms") / 1000.0).cast(TimestampType())
        elif "timestamp" in df.columns:
            event_ts_col = F.to_timestamp(F.col("timestamp"))
        elif "kafka_timestamp" in df.columns:
            event_ts_col = F.to_timestamp(F.col("kafka_timestamp"))
        else:
            event_ts_col = F.lit(None).cast(TimestampType())

        # Fallback to _ingested_at when event timestamp is null
        resolved_event_ts = F.coalesce(event_ts_col, F.col("_ingested_at"))

        df = (
            df.withColumn("year", F.year(resolved_event_ts).cast(IntegerType()))
            .withColumn("month", F.month(resolved_event_ts).cast(IntegerType()))
            .withColumn("day", F.dayofmonth(resolved_event_ts).cast(IntegerType()))
        )

        if self.include_hour_partition:
            df = df.withColumn("hour", F.hour(resolved_event_ts).cast(IntegerType()))

        # Ensure top-level 'topic' column is available for partitionBy('topic', 'year', 'month', 'day')
        if "topic" not in df.columns:
            df = df.withColumn("topic", F.col("_source_topic"))

        # Optional domain payload unpacking
        if self.unpack_payload and self.domain_schema is not None and "payload_json" in df.columns:
            unpacked_struct = F.from_json(F.col("payload_json"), self.domain_schema)
            for field in self.domain_schema.fields:
                if field.name not in df.columns:
                    df = df.withColumn(field.name, unpacked_struct.getItem(field.name))

        # Enforce contract validation
        if self.validate_contract:
            validate_bronze_schema_or_raise(
                df,
                require_partition_columns=True,
                check_data_types=True,
            )

        logger.info(
            "Bronze technical metadata enrichment completed successfully",
            extra={
                "batch_id": effective_batch_id,
                "topic": effective_topic,
                "columns": df.columns,
            },
        )
        return df


def enrich_with_bronze_envelope(
    df: DataFrame,
    batch_id: str,
    topic: str | None = None,
    processing_host: str | None = None,
    schema_version_lookup: dict[int, int] | None = None,
    domain_schema: StructType | None = None,
    unpack_payload: bool = False,
    include_hour_partition: bool = True,
    validate_contract: bool = True,
) -> DataFrame:
    """Enrich DataFrame with canonical Bronze technical metadata envelope and partition columns.

    Args:
        df: Input PySpark DataFrame.
        batch_id: Execution batch UUID.
        topic: Kafka source topic name.
        processing_host: Worker hostname.
        schema_version_lookup: Schema ID to version mapping dictionary.
        domain_schema: Optional StructType for domain payload schema.
        unpack_payload: Whether to unpack domain fields from payload_json non-destructively.
        include_hour_partition: Whether to include hour in partition columns.
        validate_contract: Whether to enforce validate_bronze_schema_or_raise().

    Returns:
        Enriched PySpark DataFrame.
    """
    enricher = BronzeMetadataEnricher(
        batch_id=batch_id,
        topic=topic,
        processing_host=processing_host,
        schema_version_lookup=schema_version_lookup,
        domain_schema=domain_schema,
        unpack_payload=unpack_payload,
        include_hour_partition=include_hour_partition,
        validate_contract=validate_contract,
    )
    return enricher.enrich(df)
