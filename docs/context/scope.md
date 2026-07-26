# Scope and Project Context: Marketplace Sandbox

## 1. Context
This project is an end-to-end data platform developed as a practical lab and portfolio. It simulates the analytical ecosystem of a real-time marketplace. The goal is to demonstrate the practical and integrated application of modern Data Engineering tools, covering the entire data lifecycle: from generation and ingestion to analytical modeling and cloud infrastructure.

## 2. Project Scope

### In-Scope
- **Infrastructure and DevOps:** Local environment configuration (Docker) and cloud deployment (AWS) using infrastructure as code (Terraform), with automated CI/CD pipelines and active monitoring (Prometheus and Grafana).
- **Continuous Ingestion:** Development of Python producers simulating transactions and navigation, sending data streams to Apache Kafka with structural validation via Kafka Schema Registry.
- **Processing and Data Lakehouse:** Event consumption via PySpark (initially local and later on Databricks) and persistence in Amazon S3 (locally simulated with MinIO) using the medallion architecture (Bronze and Silver layers).
- **Modeling and Data Warehouse:** Orchestration of batch routines with Apache Airflow for loading into AWS Redshift, modeling the Gold layer (Star Schema) via dbt.
- **DataOps and Governance:** Implementation of data quality tests, PII (sensitive data) anonymization, and data lineage tracking using dbt Docs and OpenLineage.

### Out-of-Scope
- User interfaces (Front-end/UI) or mobile applications for the marketplace.
- Real integrations with payment gateways or external transactional systems.
- Training or deployment of Machine Learning/AI models (strict focus on infrastructure and structural data engineering).

## 3. Base Tech Stack
- **Languages:** Python, SQL
- **Messaging, Ingestion, and Processing:** Apache Kafka, Confluent Schema Registry, Apache Spark (PySpark)
- **Storage and Databases:** Amazon S3 (local MinIO), AWS Redshift, PostgreSQL (used as a simulated source transactional database and as the metadata database for Apache Airflow)
- **Orchestration, ELT, and Governance:** Apache Airflow, dbt, OpenLineage
- **Infrastructure, Cloud, and Observability:** Docker, Terraform, Git, Prometheus, Grafana, AWS Lambda, AWS ECS, Databricks
