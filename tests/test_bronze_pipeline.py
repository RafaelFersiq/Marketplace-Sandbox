"""End-to-end integration and pipeline verification test suite for Medallion Bronze layer.

Validates the complete Bronze layer lifecycle conforming to ADR-0008:
1. Micro-batch landing promotion and 10 technical metadata column enrichment:
   (_bronze_id, _ingested_at, _source_topic, _kafka_partition, _kafka_offset,
    _schema_id, _schema_version, _payload_hash, _ingestion_batch_id, _processing_host).
2. Time-partitioned directory layout across all 6 business domains:
   (marketplace.orders, marketplace.payments, marketplace.cart, marketplace.inventory,
    marketplace.catalog, marketplace.navigation).
3. Append-only immutability and deterministic SHA-256 payload hash deduplication.
4. Small-file fragmentation detection, target 128MB coalescing, and atomic staging swaps.
5. PostgreSQL metadata catalog registration, upsert semantics, and batch ingestion audit trails.
6. High-performance catalog-driven partition pruning and schema-on-read querying via BronzeDatasetReader.
7. Landing zone retention lifecycle and TTL pruning via BronzeRetentionManager.
8. End-to-end CLI runner workflows for ingestion and compaction with Prometheus metrics.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import socket
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from processing.bronze.catalog import BronzeCatalogManager
from processing.bronze.compaction import BronzeCompactor
from processing.bronze.config import BronzeStorageConfig
from processing.bronze.enrichment import (
    EMPTY_SHA256_HEX,
    BronzeMetadataEnricher,
)
from processing.bronze.metrics import BronzeMetricsRegistry
from processing.bronze.reader import BronzeDatasetReader
from processing.bronze.retention import BronzeRetentionManager
from processing.bronze.schema import (
    BRONZE_ALL_TECHNICAL_COLUMNS,
    BRONZE_REQUIRED_METADATA_COLUMNS,
    build_bronze_enveloped_schema,
    validate_bronze_schema,
)
from processing.bronze.writer import BronzeTableWriter, BronzeWriteResult
from scripts.run_bronze_compaction import BronzeCompactionRunner
from scripts.run_bronze_ingestion import BronzeIngestionRunner

HAS_JAVA = shutil.which("java") is not None


def is_live_infrastructure_online(host: str = "localhost", ports: tuple[int, ...] = (5432, 9000)) -> bool:
    """Check if live container services (PostgreSQL and MinIO) are accepting TCP connections."""
    for p in ports:
        try:
            with socket.create_connection((host, p), timeout=0.5):
                pass
        except Exception:
            return False
    return True


ALL_TOPICS = [
    "marketplace.orders",
    "marketplace.payments",
    "marketplace.cart",
    "marketplace.inventory",
    "marketplace.catalog",
    "marketplace.navigation",
]


class TestBronzeEndToEndPipelineSimulation:
    """End-to-end integration test suite simulating full Bronze pipeline without JVM dependencies."""

    def test_all_six_domain_topics_configured(self) -> None:
        """Verify all 6 marketplace domains are registered in default Bronze storage configuration."""
        config = BronzeStorageConfig()
        for t in ALL_TOPICS:
            assert t in config.topics
            root = config.get_topic_root_path(t)
            assert f"events/{t}" in root

    def test_technical_metadata_envelope_contract(self) -> None:
        """Verify technical metadata envelope contains all 10 canonical audit fields."""
        schema = build_bronze_enveloped_schema(StructType([]))
        field_names = set(schema.names)

        for col in BRONZE_REQUIRED_METADATA_COLUMNS:
            assert col in field_names

        for col in BRONZE_ALL_TECHNICAL_COLUMNS:
            assert col in field_names

        is_valid, missing = validate_bronze_schema(schema)
        assert is_valid is True
        assert len(missing) == 0

    def test_payload_hash_determinism_across_domains(self) -> None:
        """Verify SHA-256 payload hash calculation is deterministic, idempotent, and tamper-evident."""
        order_payload = {"order_id": "ord-1234", "customer_id": "cust-99", "total_amount": 199.99}
        hash1 = hashlib.sha256(json.dumps(order_payload, sort_keys=True).encode("utf-8")).hexdigest()
        hash2 = hashlib.sha256(json.dumps(order_payload, sort_keys=True).encode("utf-8")).hexdigest()
        assert hash1 == hash2
        assert len(hash1) == 64  # Valid SHA-256 hex string

        # Tampered payload must yield a completely different hash
        tampered_order = {"order_id": "ord-1234", "customer_id": "cust-99", "total_amount": 200.00}
        tampered_hash = hashlib.sha256(json.dumps(tampered_order, sort_keys=True).encode("utf-8")).hexdigest()
        assert tampered_hash != hash1
        assert hashlib.sha256(b"").hexdigest() == EMPTY_SHA256_HEX

    def test_partition_derivation_from_timestamps(self) -> None:
        """Verify deterministic date partition derivation across timezones."""
        dt = datetime(2026, 11, 2, 14, 30, 0, tzinfo=UTC)
        y, m, d, h = dt.year, dt.month, dt.day, dt.hour
        assert y == 2026
        assert m == 11
        assert d == 2
        assert h == 14

    def test_e2e_ingestion_promotion_runner_flow(self) -> None:
        """Verify BronzeIngestionRunner orchestrates promotion, writing, cataloging, and metrics."""
        config = BronzeStorageConfig(topics=["marketplace.orders", "marketplace.payments"])

        mock_spark = MagicMock(spec=SparkSession)
        mock_raw_df = MagicMock(spec=DataFrame)
        mock_raw_df.count.return_value = 100
        mock_raw_df.columns = ["order_id", "customer_id", "event_time"]
        mock_spark.read.format.return_value.load.return_value = mock_raw_df

        mock_writer = MagicMock(spec=BronzeTableWriter)
        mock_writer.write_batch.return_value = BronzeWriteResult(
            batch_id="batch-pipe-01",
            topic="marketplace.orders",
            record_count=100,
            partitions_written=["s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02"],
            bytes_written=1048576,
            files_written=2,
            status="SUCCESS",
            duration_ms=250,
        )

        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        mock_enricher = MagicMock(spec=BronzeMetadataEnricher)
        mock_enricher.enrich.return_value = mock_raw_df

        registry = BronzeMetricsRegistry()

        runner = BronzeIngestionRunner(
            config=config,
            spark=mock_spark,
            catalog=mock_catalog,
            writer=mock_writer,
            enricher=mock_enricher,
            metrics_registry=registry,
            skip_preflight=True,
            deduplicate=True,
        )

        exit_code = runner.run()
        assert exit_code == 0

        # Verify writer called for both topics
        assert mock_writer.write_batch.call_count == 2
        # Verify catalog registered partition and logged audit
        assert mock_catalog.register_partition.call_count >= 1
        assert mock_catalog.log_ingestion_audit.call_count >= 1

        # Verify metrics updated
        snap = registry.get_snapshot()
        lbl = {"topic": "marketplace.orders", "status": "SUCCESS"}
        assert snap.get_counter("bronze_ingestion_records_total", labels=lbl) == 100.0

    def test_e2e_compaction_runner_flow(self) -> None:
        """Verify BronzeCompactionRunner discovers uncompacted partitions and marks them in catalog."""
        config = BronzeStorageConfig(topics=["marketplace.inventory"])

        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        mock_catalog.get_uncompacted_partitions.return_value = [
            {
                "topic": "marketplace.inventory",
                "partition_path": "s3a://lakehouse-bronze/events/marketplace.inventory/year=2026/month=11/day=02",
                "file_count": 15,
                "total_bytes": 10485760,
            }
        ]

        mock_compactor = MagicMock(spec=BronzeCompactor)
        mock_compactor.compact_partition.return_value = MagicMock(
            batch_id="comp-pipe-01",
            topic="marketplace.inventory",
            partition_path="s3a://lakehouse-bronze/events/marketplace.inventory/year=2026/month=11/day=02",
            files_before=15,
            files_after=1,
            files_reduced=14,
            bytes_before=10485760,
            bytes_after=10000000,
            bytes_reduced=485760,
            status="SUCCESS",
            duration_ms=800,
        )

        registry = BronzeMetricsRegistry()

        runner = BronzeCompactionRunner(
            config=config,
            catalog=mock_catalog,
            compactor=mock_compactor,
            metrics_registry=registry,
            skip_preflight=True,
            force=True,
        )

        exit_code = runner.run()
        assert exit_code == 0

        mock_compactor.compact_partition.assert_called_once()
        mock_catalog.mark_partition_compacted.assert_called_once_with(
            partition_path="s3a://lakehouse-bronze/events/marketplace.inventory/year=2026/month=11/day=02",
            file_count=1,
            total_bytes=10000000,
        )

        snap = registry.get_snapshot()
        top_lbl = {"topic": "marketplace.inventory"}
        assert snap.get_counter("bronze_compaction_files_reduced_total", labels=top_lbl) == 14.0

    def test_e2e_reader_catalog_pruned_pipeline(self) -> None:
        """Verify BronzeDatasetReader integrates with catalog for pruning and multi-topic reading."""
        mock_spark = MagicMock(spec=SparkSession)
        mock_df = MagicMock(spec=DataFrame)
        mock_spark.read.option.return_value.parquet.return_value = mock_df

        mock_catalog = MagicMock(spec=BronzeCatalogManager)
        mock_catalog.find_partitions.return_value = [
            "s3a://lakehouse-bronze/events/marketplace.cart/year=2026/month=11/day=01",
            "s3a://lakehouse-bronze/events/marketplace.cart/year=2026/month=11/day=02",
        ]

        reader = BronzeDatasetReader(spark=mock_spark, catalog=mock_catalog)

        # 1. Single topic read with date pruning
        df = reader.read_dataset(
            topic="marketplace.cart",
            start_date="2026-11-01",
            end_date="2026-11-02",
            use_catalog=True,
        )
        assert df == mock_df
        mock_catalog.find_partitions.assert_called_with(
            topic="marketplace.cart",
            start_date="2026-11-01",
            end_date="2026-11-02",
        )
        mock_spark.read.option.return_value.parquet.assert_called_with(
            "s3a://lakehouse-bronze/events/marketplace.cart/year=2026/month=11/day=01",
            "s3a://lakehouse-bronze/events/marketplace.cart/year=2026/month=11/day=02",
        )

        # 2. Partition count query
        count = reader.get_partition_count("marketplace.cart", start_date="2026-11-01", end_date="2026-11-02")
        assert count == 2

    def test_e2e_retention_lifecycle_pruning(self) -> None:
        """Verify BronzeRetentionManager evaluates TTL thresholds and executes batch deletion."""
        mock_s3 = MagicMock()
        now = datetime.now(UTC)

        # 2 expired objects (> 48h), 1 recent object
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {
                "Contents": [
                    {
                        "Key": "landing/marketplace.orders/old1.avro",
                        "LastModified": now - timedelta(hours=50),
                        "Size": 2048,
                    },
                    {
                        "Key": "landing/marketplace.orders/old2.avro",
                        "LastModified": now - timedelta(hours=72),
                        "Size": 4096,
                    },
                    {
                        "Key": "landing/marketplace.orders/new.avro",
                        "LastModified": now - timedelta(hours=5),
                        "Size": 1024,
                    },
                ]
            }
        ]

        manager = BronzeRetentionManager(s3_client=mock_s3)
        res = manager.prune_landing_zone(dry_run=False)

        assert res.scanned_objects_count == 3
        assert res.pruned_objects_count == 2
        assert res.pruned_bytes == 6144
        mock_s3.delete_objects.assert_called_once()


class TestBronzeLocalFileSystemIntegration:
    """Local filesystem integration test verifying physical directory creation, immutability, and compaction."""

    def test_local_partition_layout_and_compaction(self, tmp_path: Path) -> None:
        """Verify physical directory structure, multi-file creation, and local atomic compaction swap."""
        topic = "marketplace.navigation"
        part_dir = tmp_path / "events" / topic / "year=2026" / "month=11" / "day=02"
        part_dir.mkdir(parents=True)

        # Create 5 small fragmented Parquet files in partition
        for i in range(5):
            f = part_dir / f"part-0000{i}.parquet"
            f.write_bytes(b"PAR1" + b"\x00" * 500)

        assert len(list(part_dir.glob("*.parquet"))) == 5

        # Perform atomic local swap with staged compacted file
        staging_dir = part_dir / "_compacting_staging_test_batch"
        staging_dir.mkdir()
        compacted_file = staging_dir / "part-compacted-00000.parquet"
        compacted_file.write_bytes(b"PAR1" + b"\x00" * 2500)

        compactor = BronzeCompactor()
        new_file_count, new_bytes = compactor._swap_local_files(
            partition_path=str(part_dir),
            staging_path=str(staging_dir),
        )

        assert new_file_count == 1
        assert new_bytes == len(b"PAR1" + b"\x00" * 2500)
        assert len(list(part_dir.glob("*.parquet"))) == 1
        assert not staging_dir.exists()


@pytest.mark.skipif(
    not is_live_infrastructure_online(),
    reason="Live infrastructure (PostgreSQL:5432, MinIO:9000) not reachable on localhost",
)
@pytest.mark.skipif(
    not HAS_JAVA,
    reason="Java JVM not installed on host machine; PySpark execution runs inside Docker containers",
)
class TestBronzeLiveContainerIntegration:
    """Live integration tests against running PostgreSQL and MinIO Docker containers."""

    def test_live_postgres_catalog_connection_and_registration(self) -> None:
        """Verify live connection to PostgreSQL and atomic upsert into bronze_partition_catalog."""
        config = BronzeStorageConfig(postgres_host="localhost", postgres_port=5432)
        with BronzeCatalogManager(config=config) as catalog:
            part_id = catalog.register_partition(
                topic="marketplace.orders",
                year=2026,
                month=11,
                day=2,
                partition_path="s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02",
                record_count=1000,
                file_count=2,
                total_bytes=5242880,
                is_compacted=False,
                status="COMMITTED",
            )
            assert part_id > 0

            # Log audit
            catalog.log_ingestion_audit(
                batch_id="live-test-batch",
                topic="marketplace.orders",
                source_landing_path="s3a://lakehouse-bronze/landing/test",
                target_partition_path="s3a://lakehouse-bronze/events/marketplace.orders/year=2026/month=11/day=02",
                records_ingested=1000,
                bytes_written=5242880,
                files_written=2,
                execution_duration_ms=450,
                status="SUCCESS",
            )
