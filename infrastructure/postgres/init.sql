-- ============================================================================
-- Infrastructure Initialization Script for PostgreSQL
-- Target Container Path: /docker-entrypoint-initdb.d/init.sql
-- Database context on initial entry: 'marketplace' (default database)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Apache Airflow Metadata Database Setup
-- ----------------------------------------------------------------------------
-- Create dedicated database 'airflow' for storing Apache Airflow orchestration metadata.
CREATE DATABASE airflow;

-- Grant all administrative privileges on database 'airflow' to superuser 'postgres'.
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;

-- ----------------------------------------------------------------------------
-- 2. Medallion & Transactional Schemas Setup in Primary Database ('marketplace')
-- ----------------------------------------------------------------------------
-- Create schema 'transactional' for raw relational marketplace transaction simulation.
CREATE SCHEMA IF NOT EXISTS transactional;

-- Create schema 'staging' for initial dbt data cleaning, casting, and renaming layer.
CREATE SCHEMA IF NOT EXISTS staging;

-- Create schema 'intermediate' for dbt pre-aggregations and business logic joins.
CREATE SCHEMA IF NOT EXISTS intermediate;

-- Create schema 'marts' for Gold layer dimensional star-schema analytical tables.
CREATE SCHEMA IF NOT EXISTS marts;

-- ----------------------------------------------------------------------------
-- 3. Lakehouse Metadata Catalog Setup in Primary Database ('marketplace')
-- ----------------------------------------------------------------------------
-- Create schema 'lakehouse_metadata' for cataloging partitions, batch audit trails,
-- data compaction state, and dataset lineage across Medallion lakehouse tiers.
CREATE SCHEMA IF NOT EXISTS lakehouse_metadata;

COMMENT ON SCHEMA lakehouse_metadata IS 'Catalog schema for tracking Lakehouse metadata, partition registrations, and batch ingestion audit trails.';

-- Grant schema privileges to administrative user
GRANT ALL ON SCHEMA lakehouse_metadata TO postgres;

-- ----------------------------------------------------------------------------
-- 3.1. Bronze Partition Catalog Table
-- ----------------------------------------------------------------------------
-- Tracks physical partition lifecycle, data volume metrics, offset intervals,
-- and compaction status in the S3A/MinIO lakehouse-bronze bucket.
CREATE TABLE IF NOT EXISTS lakehouse_metadata.bronze_partition_catalog (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(100) NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    partition_path VARCHAR(500) NOT NULL,
    record_count BIGINT NOT NULL DEFAULT 0,
    file_count INT NOT NULL DEFAULT 0,
    total_bytes BIGINT NOT NULL DEFAULT 0,
    min_event_timestamp TIMESTAMPTZ,
    max_event_timestamp TIMESTAMPTZ,
    min_kafka_offset BIGINT,
    max_kafka_offset BIGINT,
    is_compacted BOOLEAN NOT NULL DEFAULT FALSE,
    last_compacted_at TIMESTAMPTZ,
    status VARCHAR(50) NOT NULL DEFAULT 'COMMITTED',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_bronze_partition UNIQUE (topic, year, month, day)
);

COMMENT ON TABLE lakehouse_metadata.bronze_partition_catalog IS 'Catalog of registered Medallion Bronze layer partitions with file, volume, offset, and compaction metrics.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.id IS 'Synthetic surrogate primary key.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.topic IS 'Source Kafka topic / business domain identifier.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.year IS 'UTC partition year (YYYY).';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.month IS 'UTC partition month (1-12).';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.day IS 'UTC partition day (1-31).';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.partition_path IS 'S3A URI of partition directory (e.g. s3a://lakehouse-bronze/events/{topic}/year=.../).';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.record_count IS 'Total count of records contained in this partition.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.file_count IS 'Total count of Parquet files in this partition.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.total_bytes IS 'Total aggregate byte size of Parquet files in this partition.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.min_event_timestamp IS 'Minimum event payload timestamp observed in partition.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.max_event_timestamp IS 'Maximum event payload timestamp observed in partition.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.min_kafka_offset IS 'Minimum Kafka offset ingested into partition.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.max_kafka_offset IS 'Maximum Kafka offset ingested into partition.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.is_compacted IS 'Flag indicating whether partition small files have undergone coalescing/compaction.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.last_compacted_at IS 'Timestamp of most recent compaction execution.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.status IS 'Lifecycle state: COMMITTED, COMPACTING, PURGED, etc.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.created_at IS 'Timestamp when partition was first registered in catalog.';
COMMENT ON COLUMN lakehouse_metadata.bronze_partition_catalog.updated_at IS 'Timestamp when partition catalog metadata was last modified.';

-- Indexes for Bronze partition discovery and maintenance queries
CREATE INDEX IF NOT EXISTS idx_bronze_partition_topic_date 
    ON lakehouse_metadata.bronze_partition_catalog (topic, year, month, day);

CREATE INDEX IF NOT EXISTS idx_bronze_partition_status 
    ON lakehouse_metadata.bronze_partition_catalog (status);

CREATE INDEX IF NOT EXISTS idx_bronze_partition_is_compacted 
    ON lakehouse_metadata.bronze_partition_catalog (is_compacted);

-- ----------------------------------------------------------------------------
-- 3.2. Automatic updated_at Trigger for Bronze Partition Catalog
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lakehouse_metadata.trg_fn_update_bronze_partition_catalog_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_bronze_partition_catalog_set_updated_at 
    ON lakehouse_metadata.bronze_partition_catalog;

CREATE TRIGGER trg_bronze_partition_catalog_set_updated_at
    BEFORE UPDATE ON lakehouse_metadata.bronze_partition_catalog
    FOR EACH ROW
    EXECUTE FUNCTION lakehouse_metadata.trg_fn_update_bronze_partition_catalog_updated_at();

-- ----------------------------------------------------------------------------
-- 3.3. Bronze Ingestion Audit Table
-- ----------------------------------------------------------------------------
-- Captures immutable execution telemetry, record counts, schema evolutions,
-- byte throughput, and error diagnostics per ingestion batch.
CREATE TABLE IF NOT EXISTS lakehouse_metadata.bronze_ingestion_audit (
    batch_id VARCHAR(100) PRIMARY KEY,
    topic VARCHAR(100) NOT NULL,
    source_landing_path VARCHAR(500),
    target_partition_path VARCHAR(500),
    records_ingested BIGINT NOT NULL,
    records_failed BIGINT NOT NULL DEFAULT 0,
    bytes_written BIGINT NOT NULL DEFAULT 0,
    files_written INT NOT NULL DEFAULT 0,
    schema_ids_observed INT[],
    min_event_timestamp TIMESTAMPTZ,
    max_event_timestamp TIMESTAMPTZ,
    execution_duration_ms BIGINT,
    status VARCHAR(50) NOT NULL DEFAULT 'SUCCESS',
    error_message TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE lakehouse_metadata.bronze_ingestion_audit IS 'Audit trail tracking individual batch ingestion runs into the Medallion Bronze layer.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.batch_id IS 'Unique ingestion batch run identifier (UUID or deterministic batch timestamp).';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.topic IS 'Kafka source topic ingested.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.source_landing_path IS 'S3A staging/landing path processed during batch run.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.target_partition_path IS 'S3A destination Bronze partition path written to.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.records_ingested IS 'Number of records successfully processed and written to Bronze.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.records_failed IS 'Number of records rejected or failed validation during batch.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.bytes_written IS 'Total bytes written to disk in this ingestion run.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.files_written IS 'Count of Parquet files produced in target partition.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.schema_ids_observed IS 'Array of Confluent Schema Registry schema IDs observed in this batch.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.min_event_timestamp IS 'Minimum event payload timestamp observed in batch.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.max_event_timestamp IS 'Maximum event payload timestamp observed in batch.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.execution_duration_ms IS 'Total wall-clock execution duration in milliseconds.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.status IS 'Ingestion status (SUCCESS, PARTIAL, FAILED).';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.error_message IS 'Detailed error message and stack trace if status is FAILED.';
COMMENT ON COLUMN lakehouse_metadata.bronze_ingestion_audit.ingested_at IS 'Timestamp when batch execution finished and was recorded.';

-- Indexes for audit query acceleration and SLA monitoring
CREATE INDEX IF NOT EXISTS idx_bronze_ingestion_audit_topic_ingested_at 
    ON lakehouse_metadata.bronze_ingestion_audit (topic, ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_bronze_ingestion_audit_status 
    ON lakehouse_metadata.bronze_ingestion_audit (status);

-- ----------------------------------------------------------------------------
-- 3.4. Schema Permissions & Role Privileges
-- ----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA lakehouse_metadata TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA lakehouse_metadata TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA lakehouse_metadata GRANT ALL ON TABLES TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA lakehouse_metadata GRANT ALL ON SEQUENCES TO postgres;
