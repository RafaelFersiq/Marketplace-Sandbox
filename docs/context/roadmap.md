# Implementation Roadmap — Real-Time Marketplace Data Platform

## Step 1 — Initial Project Structuring
In this step, the goal is to establish the technical foundation of the platform.
- Create the project directory structure, clearly separating the components of infrastructure, ingestion, processing, transformation, observability, testing, and documentation.
- Configure the repository with professional version control, defining a branching strategy for development, integration, and production (**advanced Git usage**).
- Configure a local environment based on containers, ensuring isolation and reproducibility (**Docker**).
- Spin up the fundamental services of the platform:
  - Apache Kafka
  - Confluent Schema Registry (for real-time data contract management and evolution)
  - Apache Spark
  - Apache Airflow
  - PostgreSQL (locally configured with multiple databases/schemas to act in a dual role: as the metadata repository for Apache Airflow and to simulate a relational source transactional database of the marketplace)
  - MinIO (Simulating **Amazon S3** locally)
  - Prometheus
  - Grafana
- Configure initial validation automation (**CI/CD**):
  - lint
  - automated tests
  - build validation
By the end of this step, the entire base platform must be operational locally.

## Step 2 — Business Domain Modeling
Define the functional context of the marketplace.
- Model the main business events that will be produced by the platform:
  - order creation
  - payment updates
  - cart events
  - stock updates
  - user navigation
  - catalog changes
- Define data contracts for each event, registering them in the **Kafka Schema Registry** to ensure structural integrity and safe message evolution.
- Specify:
  - schema (Avro / JSON Schema)
  - data types
  - requirements (nullability/required fields)
  - validation rules
  - serialization patterns
- Formally document all events.
By the end of this step, the platform must have a clear data entry specification with validated and cataloged schemas.

## Step 3 — Building Event Producers
Implement simulator services responsible for generating marketplace data.
- Create independent producers (**developed in Python**) representing:
  - checkout system
  - payments system
  - navigation service
  - inventory service
  - anti-fraud service
- Each producer must publish events to dedicated topics (**streaming ingestion pipelines via Apache Kafka**).
- Implement realistic scenarios:
  - variable volume
  - traffic bursts
  - delayed events
  - duplications
  - invalid events
  - intermittent failures
- Add structured logs for traceability.
By the end of this step, the platform must generate a continuous stream of realistic events.

## Step 4 — Streaming Ingestion Pipeline
Build the real-time ingestion layer.
- Implement consumers using **PySpark / Apache Spark** Structured Streaming.
  > [!NOTE]
  > The development and execution of this step initially occur in the local Docker environment. The actual transition to the cloud via **Databricks** will take place only in **Step 13**.
- Create pipelines for:
  - reading Kafka topics with structural validation based on schemas registered in the **Kafka Schema Registry**
  - deserialization
  - structural validation
  - error handling (e.g., routing to a Dead Letter Queue)
  - checkpointing
  - initial persistence
- Implement fault tolerance.
- Configure automatic recovery mechanisms.
- Add support for out-of-order events using watermarking.
By the end of this step, events must be continuously consumed and reliably persisted locally.

## Step 5 — Bronze Layer Construction
Implement the canonical raw data layer (**Data Lakes / medallion architecture**).
- Promote raw streaming micro-batches from the landing zone into canonical columnar storage (**Apache Parquet**).
- Enrich records with non-destructive technical audit metadata (ingestion timestamps, Kafka offsets/partitions, Schema Registry IDs/versions, and deterministic payload hashes).
- Define hierarchical time-based partitioning (`topic / year / month / day`) optimized for distributed reading and partition pruning.
- Ensure append-only immutability of ingested data.
- Implement small-file compaction routines to coalesce fragmented streaming micro-batch files into optimal target sizes.
- Register committed partitions and ingestion watermarks in a relational metadata catalog (`lakehouse_metadata` in PostgreSQL) and enforce landing retention/TTL policies.
By the end of this step, the platform must have reliable, cataloged, and scalable raw storage.

## Step 6 — Silver Layer Construction
Implement refined lakehouse processing and relational warehouse synchronization (**Data Lakes / medallion architecture**).
- Seed and maintain simulated relational OLTP reference data in the PostgreSQL `transactional` schema (e.g., master records for customers with PII, sellers, product catalog, and categories) to support cross-source enrichment and reconciliation.
- Execute in **PySpark**:
  - schema normalization, cleaning, and type standardization
  - cross-batch deduplication and state resolution
  - enrichment by joining streaming events with OLTP reference snapshots
  - reconciliation between streaming telemetry and transactional state
- Apply domain business rules and resolve structural or semantic inconsistencies.
- Route invalid or unrecoverable records to a Silver quarantine area.
- **Governance:** Apply PII anonymization/masking rules (e.g., hashing/redacting tax IDs, emails, IP addresses, and street details), privacy policies (GDPR/LGPD compliance), and sensitive data lifecycle management.
- Implement a **Silver-to-Warehouse Analytical Loader** (simulating **AWS Redshift `COPY`** locally by loading refined Silver Parquet datasets from MinIO into the PostgreSQL `silver` schema) to bridge the Data Lakehouse and the relational Data Warehouse.
- Document all applied transformations.
By the end of this step, refined data must be persisted in the Silver Lakehouse layer and synchronized into the analytical database for downstream consumption.

## Step 7 — dbt Setup and Initial Layers (Staging and Intermediate)
Introduce dbt Core as the analytical transformation engine and orchestrate the first preparation steps.
- Configure the dbt project pointing initially to the local PostgreSQL database (consuming the synchronized `silver` and `transactional` source schemas, fully parameterized for **AWS Redshift**).
- Structure the `staging` layer using dbt and SQL to perform basic cleaning, field renaming, and initial typing of data coming from the Silver layer and transactional sources.
- Structure the `intermediate` layer for pre-aggregations, key handling, entity stitching, and complex business joins.
- Configure source freshness, schema tests, and documentation for primary data sources.
By the end of this step, the dbt project must be structured, tested, and connected to the local analytical database.

## Step 8 — Dimensional Modeling (Gold Layer) and Data Lineage via dbt
Build the analytical presentation layer (Gold) by modeling the final tables that will be made available in AWS Redshift (simulated locally in this step).
- Create models for the `marts` layer containing Fact tables (e.g., `fct_orders`, `fct_navigation`) and Dimension tables (e.g., `dim_customers`, `dim_products`, `dim_sellers`, `dim_time`) structured in a multidimensional model (**Star Schema / Snowflake Schema**).
- Use dbt to materialize the fact and dimension tables directly in the analytical Redshift/PostgreSQL database (`marts` schema).
- Build and document consolidated business metrics:
  - GMV (Gross Merchandise Value)
  - average ticket
  - conversion rate and cart abandonment
  - active sellers
  - catalog performance
- Apply automatic dbt model contract tests (**DataOps and Engineering Best Practices**): uniqueness, non-nullability, referential integrity, accepted values, and custom singular business tests.
- Track end-to-end data lineage using native **dbt Docs** documentation and export execution metadata via **OpenLineage** for governance.
By the end of this step, the analytical dimensional layer will be built, validated, and documented with complete lineage tracking.

## Step 9 — Complete Orchestration
Build automated workflows to coordinate end-to-end pipeline tasks across the Medallion layers.
- Create DAGs responsible for (**Apache Airflow**):
  - ingestion and batch micro-triggers (including local payload processing; the transition to **AWS Lambda** serverless triggers will occur in **Step 13**)
  - lakehouse promotion and maintenance (orchestrating Landing $\rightarrow$ Bronze ingestion, small-file compaction, and Bronze $\rightarrow$ Silver refinement)
  - analytical warehouse loading (synchronizing Silver Parquet datasets into PostgreSQL/Redshift)
  - analytical transformation (orchestrating `dbt deps`, `dbt seed`, `dbt run`, and `dbt test` across staging, intermediate, and marts)
  - operational pipeline health checks and dead-letter / quarantine inspection
- Implement:
  - task retries with exponential backoff
  - execution timeouts
  - cross-DAG and task dependencies
  - storage and partition sensors
  - failure callbacks and recovery policies
- Define and monitor operational SLAs.
By the end of this step, all local pipeline executions must occur in a coordinated and end-to-end orchestrated manner.

## Step 10 — Data Quality Layer (DataOps)
Build a dedicated, cross-layer statistical data quality and anomaly detection framework integrated into the orchestrated pipelines.
- Implement continuous programmatic checks across Bronze, Silver, and Gold layers for:
  - completeness and schema drift
  - data freshness and SLA adherence
  - statistical distribution and volume anomalies
  - cross-layer consistency (e.g., Bronze vs. Silver vs. Gold record/GMV reconciliation)
  - referential and financial integrity
- Create automated alerting hooks for detected data anomalies and quarantine spikes.
- Implement a persistent data quality audit store (`lakehouse_metadata` quality history tables) to track quality scores and test outcomes over time.
- Integrate quality gate operators directly into the Apache Airflow DAGs to halt downstream promotion upon critical contract violations.
By the end of this step, the platform must have end-to-end operational data governance and automated quality gates.

## Step 11 — Observability and Monitoring
Consolidate telemetry across all infrastructure and data pipeline services.
- Expose and collect metrics (**using Prometheus**, service exporters, and **Prometheus Pushgateway** for ephemeral batch/micro-batch jobs):
  - producer and streaming throughput
  - end-to-end ingestion and batch processing latency
  - consumer lag and micro-batch durations
  - DLQ, quarantine, and task failure rates
  - infrastructure and container resource utilization (Kafka, PostgreSQL, Spark, MinIO, Airflow)
  - Bronze/Silver compaction and dbt execution times
- Build and provision declarative operational dashboards-as-code (**using Grafana** JSON provisioning).
- Configure threshold-based alerting rules in Prometheus/Grafana.
- Ensure end-to-end operational traceability across logs and metrics.
By the end of this step, all platform operations and data SLAs must be observable in real time.

## Step 12 — Infrastructure as Code
Code the target cloud infrastructure declaratively.
- Provision cloud resources using modular **Terraform**:
  - storage (**Amazon S3** buckets for Landing, Bronze, Silver, DLQ, and Artifacts)
  - computing and lakehouse processing (**AWS Lambda** and **Databricks** workspace/cluster resources)
  - analytical data warehouse (**Amazon Redshift**)
  - network and security (VPC, public/private subnets, security groups, least-privilege IAM roles)
  - messaging and orchestration (**Amazon MSK** / Kafka and **Amazon MWAA** / ECS)
  - observability and monitoring resources
- Version control the infrastructure with modular state management and environment parameterization (`dev`, `prod`).
- Validate infrastructure definitions via static analysis (`terraform fmt`, `terraform validate`, security/compliance checks) to guarantee complete reproducibility.
By the end of this step, the cloud infrastructure specification must be fully codified, modularized, and reproducible.

## Step 13 — Cloud Deployment (AWS Ecosystem) and CI/CD Pipeline
Transition the platform to cloud execution parity on AWS and Databricks with automated CI/CD governance.
- Configure cloud runtime adapters and deployment targets for components simulated locally:
  - Replace local MinIO endpoints with **Amazon S3** buckets.
  - Package PySpark streaming and Medallion jobs for managed **Databricks** clusters.
  - Package serverless ingestion/trigger handlers for **AWS Lambda**.
  - Configure dbt profiles and warehouse loaders (`COPY` command) for **Amazon Redshift**.
  - Synchronize Apache Airflow DAGs, plugins, and dependencies for cloud execution (**Amazon MWAA** or AWS ECS).
- Create automated CI/CD pipelines (via **GitHub Actions**) to:
  - Validate (`lint`/`validate`), plan (`terraform plan`), and govern infrastructure deployments (`terraform apply` supporting automated dry-run/mock verification by default and on-demand live cloud provisioning with automated teardown for cost control).
  - Package, test, and deploy DAGs, PySpark wheels, Lambda bundles, and dbt models.
- Execute end-to-end integration and cloud readiness validation tests.
- Document scale behavior, performance benchmarks, and cloud FinOps cost estimates.
By the end of this step, the platform must be cloud-ready and governed end-to-end via automated CI/CD pipelines.

## Step 14 — Technical Documentation and Portfolio
Produce comprehensive technical documentation.
- Describe:
  - architecture (data-centric systems)
  - technical decisions
  - trade-offs
  - adopted patterns
  - scalability strategies
  - fault tolerance
  - governance (accountability)
- Create diagrams.
- Produce a functional demo.
- Document obtained metrics.
By the end of this step, the project must be presentable to recruiters as a professional data engineering platform.
