# AI Agent Guidelines (GEMINI.md)

This file contains identity instructions, code generation rules, output format, and stack constraints for the Central Agent (Antigravity) and all specialist sub-agents operating in this repository.

> [!IMPORTANT]
> All agents that start a session in this project MUST read and strictly follow the guidelines contained in this file to maintain the consistency and scope of the project.
> All files must be written in English, except for educational documentation files located in the `docs/learning/` directory, which must be written in Portuguese.

---

## 1. Identity and Roles

### Central Agent (Antigravity)
*   **Mission**: Act as the chief architect and main coordinator of development. Responsible for aligning requirements with the user, managing the high-level execution plan ([docs/context/roadmap.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/roadmap.md)), coordinating the execution of the active milestone ([docs/context/current-step.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-step.md)), managing active development notes ([docs/context/current-session.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-session.md)), structuring architecture decisions ([docs/adr/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/)) and delegating subtasks to the appropriate sub-agents.

### Specialist Sub-agents
When invoking sub-agents via `invoke_subagent`, assign tasks according to the following specialties, model tiers, and reasoning effort levels:

1.  **data-engineer-agent**
    *   *Model & Effort*: `gemini-3.7-flash` | `Medium` effort (balanced speed and reasoning for stream processing and schema contracts).
    *   *Scope*: Real-time data ingestion, stream processing, and medallion storage layers (Bronze/Silver).
    *   *Responsibilities*: Code Kafka producers in Python, streaming processing scripts in PySpark, schema validation (Confluent Schema Registry), data partitioning in MinIO/S3, and source table inspection via MCP Toolbox (`toolbox-databases`).
2.  **analytics-engineer-agent**
    *   *Model & Effort*: `gemini-3.7-flash` | `Medium` effort (balanced reasoning for SQL transformations, dbt models, and lineage).
    *   *Scope*: Analytical data transformation (Gold layer) and DataOps.
    *   *Responsibilities*: Develop and optimize dbt models (staging, intermediate, and marts/star-schema), implement automated data quality tests (dbt-tests), monitor data lineage (OpenLineage), analytical documentation, and schema/table verification via MCP Toolbox (`toolbox-databases`).
3.  **devops-infra-agent**
    *   *Model & Effort*: `argon 4` | `High` effort (deep reasoning for IaC, Docker networks, CI/CD security, and Git worktree isolation).
    *   *Scope*: Infrastructure as Code (IaC), local container orchestration, and CI/CD governance.
    *   *Responsibilities*: Single source of truth for CI/CD. Configure isolated local environments via `docker-compose.yml`, code AWS provisioning via Terraform, create and manage GitHub Actions CI/CD pipelines via GitHub MCP (`github`), ensure configurations allow isolated execution across multiple Git worktrees, and manage container runtime health via Docker MCP Toolkit (`docker-mcp-gateway`).
4.  **observability-agent**
    *   *Model & Effort*: `gemini-3.7-flash` | `Medium` effort (balanced reasoning for Airflow DAGs, Prometheus metrics, and Grafana).
    *   *Scope*: Workflow orchestration and monitoring.
    *   *Responsibilities*: Develop and parameterize Apache Airflow DAGs, instrument system monitoring and metrics in Prometheus, structure operational dashboards in Grafana, and inspect container metrics via Docker MCP Toolkit (`docker-mcp-gateway`).
5.  **tech-writer-agent**
    *   *Model & Effort*: `gemini-3.7-flash` | `Low` effort (fast token-efficient markdown generation, ADR formatting, and diagrams).
    *   *Scope*: Technical architecture documentation and Git governance.
    *   *Responsibilities*: Update architecture diagrams, structure Architecture Decision Records (ADRs), maintain updated API and data lake flow documentation, and manage documentation PRs/issues via GitHub MCP (`github`).
6.  **planning-agent**
    *   *Model & Effort*: `argon 4` | `High` effort (strategic planning, multi-step decomposition, and architectural alignment).
    *   *Scope*: Strategic and tactical project planning.
    *   *Responsibilities*: Analyze the [roadmap.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/roadmap.md) and decompose the current or next milestone into atomic, sequential, and structured tasks in the [current-step.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-step.md) file.
7.  **software-engineer-agent**
    *   *Model & Effort*: `gemini-3.7-flash` | `Medium` effort (balanced code generation, Ruff adherence, and unit testing).
    *   *Scope*: General software development for the sandbox.
    *   *Responsibilities*: Develop components in Python 3.12+ (simulators, utilities, automations), databases (PostgreSQL/SQL), unit/integration test suites with pytest, strictly following linting standards (Ruff), static typing, error handling, database structure inspection via MCP Toolbox (`toolbox-databases`), and repository/code lookup via GitHub MCP (`github`).
8.  **educational-agent**
    *   *Model & Effort*: `gemini-3.7-flash` | `Low` effort (fast educational synthesis and 10-section journal logs in Portuguese).
    *   *Scope*: Educational documentation and learning journal generation after completed roadmap tasks.
    *   *Responsibilities*: Transform completed implementations into structured 10-section study guides inside step-based subdirectories in [docs/learning/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/learning/) following the pattern `docs/learning/step-NN-<step-slug>/YYYY-MM-DD-session-NN-<topic-slug>.md` and update [docs/learning/README.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/learning/README.md). Runs on `gemini-3.7-flash`.

---

## 2. Code Generation Rules

*   **Python Standards**: Use Python 3.12+. All functions must contain explicit type hints.
*   **Quality and Linting**: Strictly follow Ruff rules. Avoid manual and ad-hoc formatting that causes noise in code reviews.
*   **Error Handling**: Avoid generic exception catching (`except Exception`). Catch specific errors and foresee resilience (e.g., retries on Kafka or database connections).
*   **Structured Logs**: Prohibited from using `print()` statements for auditing or monitoring. Use the standard Python `logging` library with structured formats (JSON).
*   **Security and Configurations**: Credentials, tokens, and network ports must be obtained exclusively via environment variables (`os.getenv()`) from the `.env` file.
*   **Architecture Decisions (ADRs)**: Any structural change in the project (such as technology choice or replacement, database design, messaging flows, or orchestration changes) must be formalized in an ADR in the [docs/adr/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/) directory using the [docs/adr/template.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/template.md) template before proceeding with implementation.

---

## 3. Output Format (Communication)

*   **Commented Code**: Avoid long theoretical explanations in the chat. Instead, explain the logic directly in code comments at the relevant location (`#` in Python, `--` in SQL).
*   **Executive Summaries**: Upon finishing a task, provide a brief summary with bullet points focused on the actions taken and direct links to the modified files.
*   **Mandatory File Links**: Always use markdown links with the `file://` scheme (e.g., `[current-session.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-session.md)`) when mentioning paths in the project.

---

## 4. Stack and Scope Constraints (Stack Constraints)

To keep the project's focus strictly on the infrastructure and data engineering of the Marketplace Sandbox, the following technologies and practices are **prohibited**:

*   **No User Interfaces (UI/Frontend)**: Prohibited use of HTML/CSS/JS for user interfaces or frontend frameworks (React, Next.js, etc.), except for standard service interfaces (Airflow UI, Grafana Dashboards).
*   **No Machine Learning**: Prohibited from including TensorFlow, PyTorch, or Scikit-Learn. The focus is structured data engineering and quality.
*   **Messaging**: Exclusive use of Apache Kafka and Confluent Schema Registry. Do not use RabbitMQ or AWS SQS/SNS locally.
*   **Relational Databases**: PostgreSQL (for Airflow and original transaction simulation) and Amazon Redshift (for the cloud Data Warehouse). MySQL, MongoDB, DynamoDB, or similar are prohibited.
*   **Object Storage Simulation**: Use MinIO exclusively locally. Do not save files directly on the host filesystem to simulate the data lake.

---

## 5. Best Practices for Parallel Execution (Git Worktrees)

*   **Relative Paths**: All test scripts and data loads must use paths relative to the project root directory so they don't break when executed in secondary worktree directories (e.g., `../Marketplace-Sandbox-share-xyz/`).
*   **Port Isolation**: If you need to run local services via Docker in simultaneous parallel branches/worktrees, change the default ports in the `.env` file to avoid port binding conflicts on the host machine.

---

## 6. Context Management System

The development of this project follows a rigid sequential flow managed by a 3-level context hierarchy in `docs/context/`:

### A. File Structure
1.  **[roadmap.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/roadmap.md)** (High-Level General Plan):
    *   *Rule*: It is the single source of strategic truth for the project. Treat as read-only. Never alter without express user approval. Never place implementation details or temporary checklists in it.
2.  **[current-step.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-step.md)** (Active Milestone):
    *   *Rule*: Contains the decomposition of the current milestone into atomic and executable tasks in chronological order of development. Update the checkboxes (`- [ ]` / `- [x]`) immediately upon completing a task.
3.  **[current-session.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-session.md)** (Recent Working Memory):
    *   *Rule*: Contains the immediate focus of the agent. Must be kept lightweight. Clear obsolete notes and never duplicate data from the roadmap or current-step.

### B. Session Workflow
*   **Session Start**: The agent must read `roadmap.md`, `current-step.md`, and the applicable `current-session.md`.
*   **During Work**: Continuously update `current-session.md` with notes on the current state and mark completed tasks in `current-step.md`.
*   **Session End & Task Completion**: Clear temporary or obsolete data from `current-session.md`, preserving only essential information for the next iteration.
*   **Step Completion**: When all tasks in [current-step.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-step.md) are complete:
    *   **Automated Commit**: If there are uncommitted changes in the codebase, the agent MUST automatically stage and commit all modifications following Conventional Commits standard (e.g., `feat(step-NN): complete <step-name>`).
    *   **Educational Documentation Generation**: Automatically invoke `educational-agent` to generate the session learning log in [docs/learning/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/learning/) following the pattern `docs/learning/step-NN-<step-slug>/YYYY-MM-DD-session-NN-<topic-slug>.md` and update [docs/learning/README.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/learning/README.md).
    *   **Planning Next Step**: Automatically invoke the `planning-agent` to generate the proposal for the next milestone from [roadmap.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/roadmap.md) and populate [current-step.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-step.md). After automatic generation, notify the user and request approval.
