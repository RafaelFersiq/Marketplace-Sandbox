# Real-Time Marketplace Data Platform (Sandbox)

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Apache Kafka](https://img.shields.io/badge/streaming-Apache_Kafka-black.svg?logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/processing-PySpark_Structured_Streaming-E25A1C.svg?logo=apachespark)](https://spark.apache.org/)
[![dbt Core](https://img.shields.io/badge/transformation-dbt_Core-FF694B.svg?logo=dbt)](https://www.getdbt.com/)
[![Apache Airflow](https://img.shields.io/badge/orchestration-Apache_Airflow-017CEE.svg?logo=apacheairflow)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/containers-Docker-2496ED.svg?logo=docker)](https://www.docker.com/)

A comprehensive, production-grade Data Engineering sandbox simulating a high-throughput, event-driven e-commerce marketplace. The platform demonstrates end-to-end real-time ingestion, stream processing, Medallion Architecture (Bronze/Silver/Gold layers), analytics engineering with Star Schema modeling, automated pipeline orchestration, infrastructure-as-code, and full observability.

---

## 📌 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Key Features](#-key-features)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Environment Setup](#environment-setup)
  - [Running the Local Stack](#running-the-local-stack)
- [Development & Governance System](#-development--governance-system)
  - [Context Management](#context-management)
  - [Specialist Sub-Agents](#specialist-sub-agents)
  - [Architecture Decision Records (ADRs)](#architecture-decision-records-adrs)
- [Project Roadmap & Status](#-project-roadmap--status)
- [License](#-license)

---

## 🏗 Architecture Overview

The system processes streaming marketplace events (orders, payments, navigation, inventory updates) through a multi-tier data pipeline:

```
                          [ Event Simulators (Python) ]
                                       │
                                       ▼
                       [ Apache Kafka + Schema Registry ]
                                       │
                                       ▼
                  [ PySpark Structured Streaming Consumer ]
                                       │
                ┌──────────────────────┴──────────────────────┐
                ▼                                             ▼
       [ Bronze Layer (Raw MinIO/S3) ]               [ Dead Letter Queue ]
                │
                ▼
       [ Silver Layer (Cleaned & Standardized) ]
                │
                ▼
       [ dbt Core Transformation ]
       ├── Staging & Intermediate
       └── Gold Layer (Star Schema / Marts) ──► [ Analytical DW (PostgreSQL / Redshift) ]
                                                              │
                                                              ▼
                                                   [ Grafana Dashboards ]
```

---

## ✨ Key Features

- **Event-Driven Streaming**: Simulates real-time e-commerce events (Checkout, Payments, Navigation, Inventory, Anti-fraud) with Confluent Schema Registry enforcement.
- **Medallion Lakehouse Architecture**:
  - **Bronze**: Raw, immutable event storage with time-based partitioning.
  - **Silver**: Deduplicated, cleaned, enriched data with PII anonymization and quarantine handling.
  - **Gold**: Dimensional modeling (Star/Snowflake schema) serving business metrics like GMV, Average Ticket, and Conversion Rates.
- **Analytics Engineering with dbt**: Modular transformation models (Staging, Intermediate, Marts) with data quality tests and lineage tracking via OpenLineage / dbt Docs.
- **Workflow Orchestration**: Apache Airflow DAGs coordinating ingestion, validation, dbt runs, and monitoring routines.
- **Full Observability**: Prometheus metrics collection tracking pipeline throughput, latency, lag, and system health visualized via Grafana.
- **Infrastructure-as-Code & Containers**: Fully reproducible local stack via `docker-compose` and cloud provisioning via Terraform on AWS.

---

## 🛠 Tech Stack

| Category | Component / Tool | Role in Sandbox |
| :--- | :--- | :--- |
| **Language & Runtime** | Python 3.12+ | Event simulators, PySpark consumers, scripting |
| **Streaming / Messaging** | Apache Kafka & Schema Registry | Real-time event broker & data contract validation |
| **Stream Processing** | PySpark Structured Streaming | Real-time ingestion, validation, and Bronze/Silver persistence |
| **Object Storage** | MinIO (Local) / AWS S3 (Cloud) | Data Lake storage engine for Medallion layers |
| **Relational & OLAP DB** | PostgreSQL (Local) / AWS Redshift (Cloud) | Airflow metadata store & Analytical Data Warehouse |
| **Data Transformation** | dbt Core | Analytical modeling (Staging, Intermediate, Gold/Marts) |
| **Orchestration** | Apache Airflow | Workflow coordination, sensors, automated retries |
| **Observability** | Prometheus & Grafana | Real-time metrics monitoring and operational dashboards |
| **Infrastructure & CI/CD** | Docker, Terraform, GitHub Actions | Containerized stack, IaC, and automated linting/testing |

---

## 📂 Project Structure

```
Marketplace-Sandbox/
├── .agents/                    # Sub-agent definitions and custom skills
├── config/                     # Environment configuration templates (.env.example)
├── docs/                       # Project documentation
│   ├── adr/                    # Architecture Decision Records (ADRs)
│   ├── context/                # Context management (roadmap.md, current-step.md, current-session.md)
│   └── learning/               # Educational study guides & learning journals (in Portuguese)
├── infrastructure/             # Container orchestration and IaC
│   ├── docker-compose.yml      # Local stack (Kafka, Spark, Airflow, Postgres, MinIO, Prometheus, Grafana)
│   ├── postgres/               # Database initialization scripts (init.sql)
│   └── prometheus/             # Prometheus configuration (prometheus.yml)
├── ingestion/                  # Event simulators and Kafka producers
├── observability/              # Airflow DAGs, Prometheus metrics exporters, Grafana dashboards
├── processing/                 # PySpark streaming consumer jobs (Bronze & Silver layers)
├── tests/                      # Pytest unit & integration test suites
├── transformation/             # dbt Core project (models, seeds, macros, tests)
├── .env                        # Local environment credentials & port mappings (git-ignored)
├── GEMINI.md                   # AI Agent System Guidelines & Operating Rules
└── README.md                   # Project overview & documentation (this file)
```

---

## 🚀 Getting Started

### Prerequisites

- **Docker & Docker Compose** (v24.0+)
- **Python 3.12+**
- **Git**

### Environment Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/RafaelFersiq/Marketplace-Sandbox.git
   cd Marketplace-Sandbox
   ```

2. Copy the environment variables template:
   ```bash
   cp config/.env.example .env
   ```
   *(Optionally edit `.env` to customize ports or credentials if needed).*

### Running the Local Stack

1. Start all containerized services:
   ```bash
   docker compose -f infrastructure/docker-compose.yml up -d
   ```

2. Access the service web interfaces:
   - **Kafka UI**: [http://localhost:8080](http://localhost:8080)
   - **Schema Registry**: [http://localhost:8081](http://localhost:8081)
   - **Spark Master**: [http://localhost:8082](http://localhost:8082)
   - **Apache Airflow**: [http://localhost:8085](http://localhost:8085) *(User: `airflow` / Pass: `airflow`)*
   - **MinIO Console**: [http://localhost:9001](http://localhost:9001) *(User: `minioadmin` / Pass: `minioadmin`)*
   - **Prometheus**: [http://localhost:9090](http://localhost:9090)
   - **Grafana**: [http://localhost:3000](http://localhost:3000) *(User: `admin` / Pass: `admin`)*

---

## 🤖 Development & Governance System

This repository operates under an AI Agent Governance Framework detailed in [`GEMINI.md`](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/GEMINI.md).

### Context Management
Development progress is strictly managed via a 3-level context hierarchy in [`docs/context/`](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/):
- **[`roadmap.md`](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/roadmap.md)**: Strategic 14-step implementation plan.
- **[`current-step.md`](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-step.md)**: Decomposed atomic tasks for the active milestone.
- **[`current-session.md`](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-session.md)**: Real-time working memory of current tasks.

### Specialist Sub-Agents
Development is executed across 8 domain-specific AI sub-agents:
- `data-engineer-agent` (Ingestion & Stream Processing)
- `analytics-engineer-agent` (dbt Modeling & DataOps)
- `devops-infra-agent` (Docker, IaC, CI/CD)
- `observability-agent` (Airflow, Prometheus, Grafana)
- `tech-writer-agent` (Architecture Documentation & Diagrams)
- `planning-agent` (Milestone Planning & Decomposition)
- `software-engineer-agent` (Python Utilities & Pytest)
- `educational-agent` (Portuguese Study Guides in [`docs/learning/`](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/learning/))

### Architecture Decision Records (ADRs)
All structural choices (technology selections, database design, messaging patterns) are documented in [`docs/adr/`](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/):
- [ADR-0001: Git Branching Strategy](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/0001-git-branching-strategy.md)

---

## 📅 Project Roadmap & Status

- [x] **Step 1 — Initial Project Structuring** *(In Progress: Docker stack & base setup completed)*
- [ ] **Step 2 — Business Domain Modeling**
- [ ] **Step 3 — Building Event Producers**
- [ ] **Step 4 — Streaming Ingestion Pipeline**
- [ ] **Step 5 — Bronze Layer Construction**
- [ ] **Step 6 — Silver Layer Construction**
- [ ] **Step 7 — dbt Setup and Initial Layers (Staging/Intermediate)**
- [ ] **Step 8 — Dimensional Modeling (Gold Layer) & Data Lineage**
- [ ] **Step 9 — Complete Orchestration (Airflow)**
- [ ] **Step 10 — Data Quality Layer (DataOps)**
- [ ] **Step 11 — Observability and Monitoring**
- [ ] **Step 12 — Infrastructure as Code (Terraform)**
- [ ] **Step 13 — Cloud Deployment (AWS Ecosystem) & CI/CD Pipeline**
- [ ] **Step 14 — Technical Documentation & Portfolio**

*Note: This README is maintained continuously as new implementation steps are completed.*

---

## 📜 License

This project is open-source and intended for education, research, and portfolio demonstration purposes.
