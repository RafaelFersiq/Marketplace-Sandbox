# Current Step: Step 4 — Streaming Ingestion Pipeline

## Active Tasks

### Phase 1: Architectural Design & Infrastructure Readiness
- [ ] Write Architecture Decision Record ADR-0007 in [docs/adr/0007-streaming-ingestion-pipeline-architecture.md](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/adr/0007-streaming-ingestion-pipeline-architecture.md) documenting PySpark Structured Streaming design, Confluent Avro wire-format deserialization strategy, Schema Registry integration, Dead Letter Queue (DLQ) pattern, watermarking policy, and checkpoint-based fault tolerance
    * **Assigned Agent**: `tech-writer-agent`
- [ ] Update [pyproject.toml](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/pyproject.toml) to include `pyspark>=3.5.1` and verify/configure Spark container environment in [infrastructure/docker-compose.yml](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml) with required Spark packages (`spark-sql-kafka-0-10_2.12:3.5.1`, `spark-avro_2.12:3.5.1`, `hadoop-aws:3.3.4`, `aws-java-sdk-bundle:1.12.262`) and automated MinIO bucket initialization (`lakehouse-bronze`, `lakehouse-dlq`)
    * **Assigned Agent**: `devops-infra-agent`

### Phase 2: Core Streaming Framework & Wire-Format Deserialization
- [ ] Implement streaming configuration dataclass `SparkStreamingConfig` in [processing/streaming/config.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/processing/streaming/config.py) managing Kafka broker parameters, Schema Registry settings, topic subscriptions, trigger intervals, watermark threshold, landing paths, and checkpoint directories
    * **Assigned Agent**: `software-engineer-agent`
- [ ] Implement Confluent Avro wire-format deserializer and schema validation logic in [processing/streaming/deserializer.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/processing/streaming/deserializer.py) extracting the 5-byte header (magic byte 0x00 + 4-byte schema ID), retrieving schemas from Schema Registry with in-memory caching, decoding Avro binary payloads into PySpark DataFrame schemas, and safely tagging corrupt/malformed payloads without failing the stream
    * **Assigned Agent**: `data-engineer-agent`
- [ ] Implement Dead Letter Queue (DLQ) routing and error isolation in [processing/streaming/dlq.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/processing/streaming/dlq.py) filtering invalid/malformed records, enriching them with diagnostic metadata (`_error_type`, `_error_message`, `_raw_payload_base64`, `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_failed_at`), and routing to Kafka DLQ topic `marketplace.dlq` and/or MinIO DLQ storage
    * **Assigned Agent**: `data-engineer-agent`

### Phase 3: Watermarking, Fault Tolerance & Streaming Pipeline Orchestration
- [ ] Implement event-time watermarking and stateful deduplication in [processing/streaming/watermark.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/processing/streaming/watermark.py) parsing Avro `timestamp-millis` logical timestamps, configuring Spark Structured Streaming watermarks (e.g., 10-minute delay) to handle late-arriving events, and applying deduplication on `event_id`
    * **Assigned Agent**: `data-engineer-agent`
- [ ] Implement `StreamingIngestionPipeline` orchestrator in [processing/streaming/pipeline.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/processing/streaming/pipeline.py) unifying multi-topic Kafka consumption (`marketplace.orders`, `marketplace.payments`, `marketplace.cart`, `marketplace.inventory`, `marketplace.catalog`, `marketplace.navigation`), wire-format deserialization, watermark handling, DLQ bifurcated routing, and fault-tolerant landing persistence to MinIO (`s3a://lakehouse-bronze/landing/`) using checkpointing
    * **Assigned Agent**: `data-engineer-agent`
- [ ] Implement `StreamingMetricsListener` in [processing/streaming/metrics.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/processing/streaming/metrics.py) using Spark's `StreamingQueryListener` to capture streaming query progress metrics (input rate, processing rate, batch duration, watermark, trigger execution times) with structured JSON logging and Prometheus metric counters
    * **Assigned Agent**: `observability-agent`
- [ ] Implement resilient execution and automatic recovery loop in [processing/streaming/recovery.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/processing/streaming/recovery.py) providing exponential backoff restart on transient network/broker interruptions and graceful SIGINT/SIGTERM termination ensuring clean checkpoint commits
    * **Assigned Agent**: `software-engineer-agent`

### Phase 4: CLI Execution & Test Suites
- [ ] Implement CLI streaming runner in [scripts/run_streaming_ingestion.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/scripts/run_streaming_ingestion.py) supporting configurable CLI arguments (`--topics`, `--bootstrap-servers`, `--schema-registry-url`, `--landing-path`, `--checkpoint-dir`, `--watermark-delay`, `--trigger-interval`, `--once`, `--continuous`), pre-flight connectivity checks, and structured JSON audit logging
    * **Assigned Agent**: `software-engineer-agent`
- [ ] Create unit test suites in [tests/test_streaming_config.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/tests/test_streaming_config.py), [tests/test_streaming_deserializer.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/tests/test_streaming_deserializer.py), and [tests/test_streaming_dlq.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/tests/test_streaming_dlq.py) testing config parsing, wire-format deserialization, schema cache handling, error tagging, and DLQ payload formatting
    * **Assigned Agent**: `software-engineer-agent`
- [ ] Create end-to-end integration test suite in [tests/test_streaming_pipeline.py](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/tests/test_streaming_pipeline.py) validating the complete streaming ingestion flow against live Kafka, Schema Registry, and MinIO: producing valid and chaos events, verifying continuous consumption, checking landing persistence, confirming DLQ isolation of malformed events, and testing checkpoint recovery
    * **Assigned Agent**: `data-engineer-agent`

### Phase 5: Technical Documentation & Educational Guide
- [ ] Write comprehensive technical operations guide in [docs/streaming_ingestion_guide.md](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/streaming_ingestion_guide.md) documenting streaming ingestion architecture, Confluent wire format handling in Spark, watermarking strategy, DLQ pattern, checkpoint recovery, CLI options, and operational runbooks
    * **Assigned Agent**: `tech-writer-agent`
- [ ] Create educational session journal in [docs/learning/step-04-streaming-ingestion-pipeline/](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/learning/step-04-streaming-ingestion-pipeline/) and update [docs/learning/README.md](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/learning/README.md) synthesizing key streaming engineering concepts (at-least-once semantics, stateful streaming, Confluent wire format, watermarking, DLQ)
    * **Assigned Agent**: `educational-agent`
