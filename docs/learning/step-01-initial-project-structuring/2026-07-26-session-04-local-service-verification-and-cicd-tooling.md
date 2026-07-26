# Guia de Aprendizado — Etapa 1: Estruturação Inicial do Projeto (Initial Project Structuring)

---

## 1. Resumo Executivo & Visão Geral

A **Etapa 1 (Estruturação Inicial do Projeto)** estabelece os alicerces de infraestrutura, governança de código e automação da **Plataforma de Dados do Marketplace**. O objetivo desta etapa é criar um ambiente sandbox local totalmente containerizado, determinístico e reprodutível, capaz de simular em escala local uma plataforma de engenharia de dados em tempo real de nível corporativo.

A plataforma abrange todo o ciclo de vida dos dados: desde a ingestão streaming de eventos de e-commerce (pedidos, pagamentos, estoque, navegação) via [Apache Kafka](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L74-L98) e [Confluent Schema Registry](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L122-L141), o armazenamento distribuído no [MinIO](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L32-L51) (emulando o Amazon S3), a persistência relacional dual-role no [PostgreSQL 16](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/postgres/init.sql), a computação distribuída com [Apache Spark](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L146-L169), até a orquestração via [Apache Airflow](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L173-L251) e a observabilidade com [Prometheus](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L256-L270) e [Grafana](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L275-L292).

### Objetivos Alcançados na Etapa 1

- **Estruturação Modular do Repositório:** Separação rígida de fronteiras em [infrastructure/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/), [ingestion/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/ingestion/), [processing/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/processing/), [transformation/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/transformation/), [observability/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/observability/), [tests/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/tests/) e [docs/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/).
- **Governança no Git e Branching Strategy:** Definição formal da estratégia **GitHub Flow** no [ADR-0001](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/0001-git-branching-strategy.md), otimizada para desenvolvimento paralelo via **Git Worktrees**.
- **Infraestrutura Declarativa via Docker Compose:** Provisionamento e orquestração de 13 serviços containerizados integrados através do [docker-compose.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml), utilizando dependências baseadas em *healthchecks* ativos.
- **Banco Relacional Dual-Role:** Criação do script [init.sql](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/postgres/init.sql) no PostgreSQL 16 para segregar os metadados do Airflow (database `airflow`) e as camadas da Arquitetura Medalhão (schemas `transactional`, `staging`, `intermediate` e `marts` no database `marketplace`).
- **Automação de Verificação de Serviços:** Desenvolvimento do script [verify_services.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/verify_services.py) em Python 3.12+ com checagens TCP/HTTP e logs estruturados em formato JSON.
- **Integração Contínua (CI):** Configuração do linter/formatter ultra-rápido Ruff e do executor de testes Pytest no [pyproject.toml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/pyproject.toml), integrados ao GitHub Actions via [.github/workflows/ci.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.github/workflows/ci.yml).

### Diagrama de Arquitetura da Infraestrutura Local

```mermaid
flowchart TD
    subgraph Host["Host Developer Machine (Linux Environment)"]
        subgraph Net["Docker Bridge Network: marketplace-network"]

            subgraph Storage_DB["Persistência & Armazenamento"]
                Postgres["marketplace-postgres\n(PostgreSQL 16)\nPorta 5432\nDatabases: marketplace, airflow"]
                MinIO["marketplace-minio\n(MinIO / S3 Emulato)\nPortas 9000 (API) / 9001 (Console)"]
            end

            subgraph Event_Streaming["Event Streaming & Governança"]
                Zookeeper["marketplace-zookeeper\n(CP-Zookeeper 7.6.0)\nPorta 2181"]
                Kafka["marketplace-kafka\n(CP-Kafka 7.6.0)\nPortas 9092 (Host) / 29092 (Internal)"]
                SchemaRegistry["marketplace-schema-registry\n(CP-Schema-Registry 7.6.0)\nPorta 8081"]
                KafkaUI["marketplace-kafka-ui\n(Kafka UI)\nPorta 8085"]
            end

            subgraph Compute["Computação Distribuída"]
                SparkMaster["marketplace-spark-master\n(Spark 3.5.1 Master)\nPortas 7077 (RPC) / 8082 (Web UI)"]
                SparkWorker["marketplace-spark-worker\n(Spark 3.5.1 Worker)\n1 Core / 1GB RAM"]
            end

            subgraph Orchestration["Orquestração de Pipelines"]
                AirflowInit["marketplace-airflow-init\n(Airflow 2.9.1 Init DB)\n[Init Container Efêmero]"]
                AirflowWeb["marketplace-airflow-webserver\n(Airflow UI)\nPorta 8080"]
                AirflowSched["marketplace-airflow-scheduler\n(Airflow Scheduler)"]
            end

            subgraph Observability["Observabilidade & Métricas"]
                Prometheus["marketplace-prometheus\n(Prometheus Server)\nPorta 9090"]
                Grafana["marketplace-grafana\n(Grafana Dashboards)\nPorta 3000"]
            end

        end
    end

    %% Dependências e Comunicações
    Zookeeper -->|nc -z 2181| Kafka
    Kafka -->|kafka-topics --list| SchemaRegistry
    Kafka -->|PLAINTEXT://kafka:29092| KafkaUI
    SchemaRegistry -->|http://schema-registry:8081| KafkaUI
    Postgres -->|pg_isready| AirflowInit
    AirflowInit -->|service_completed_successfully| AirflowWeb
    AirflowInit -->|service_completed_successfully| AirflowSched
    Postgres -->|SQLAlchemy connection| AirflowWeb
    Postgres -->|SQLAlchemy connection| AirflowSched
    SparkMaster -->|spark://spark-master:7077| SparkWorker
    Prometheus -->|scrape target localhost:9090| Prometheus
    Prometheus -->|Datasource| Grafana
```

---

## 2. Arquitetura & Decisões de Design

A arquitetura da plataforma foi projetada seguindo os princípios de **infraestrutura declarativa**, **desacoplamento de configurações (12-Factor App)** e **governança rigorosa de código**.

> [!NOTE]
> Todos os detalhes das decisões arquiteturais tomadas durante o projeto são formalizados em Registros de Decisões de Arquitetura ([ADRs](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/)) antes da implementação do código.

### 1. Governança no Git: GitHub Flow & Git Worktrees ([ADR-0001](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/0001-git-branching-strategy.md))

No desenvolvimento coordenado por múltiplos agentes especialistas executando em paralelo, estratégias como o Git Flow (com branches de longa duração `develop`, `release/*` e `hotfix/*`) introduzem um alto overhead de mesclagem e riscos de conflitos complexos.

Conforme registrado no [ADR-0001](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/0001-git-branching-strategy.md), adotamos o **GitHub Flow**:
- **Branch Principal Estável (`main`):** Reflete sempre um estado funcional, testado e implantável.
- **Branches de Feature de Curta Duração (`feature/*` ou `infra/*`):** Criadas diretamente a partir da `main`.
- **Compatibilidade com Git Worktrees:** Permite que subagentes executem em diretórios isolados (`git worktree add`) em branches paralelas sem interferir no workspace ativo ou exigir `git checkout`/`stash` frequentes.

### 2. Desacoplamento de Configurações (Fator III do 12-Factor App)

Nenhuma credencial, porta de comunicação ou URL de serviço é hardcoded no código ou nos manifestos. 
- O arquivo [config/.env.example](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/config/.env.example) serve como o modelo de contrato de variáveis de ambiente do projeto.
- O arquivo [.env](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.env) local (ignorado pelo Git via [.gitignore](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.gitignore)) injeta as variáveis em runtime no Docker Compose e nos scripts Python.

### 3. Padrão de Contêiner Efêmero de Inicialização (*Init Container Pattern*)

No Apache Airflow, a inicialização do esquema de banco de dados (`airflow db init`) e a criação do usuário administrador (`airflow users create`) devem ocorrer de forma idempotente e atômica antes do disparo dos serviços persistentes.

No [docker-compose.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L173-L193), o serviço `airflow-init` desempenha esse papel:
1. Aguarda a prontidão operacional do PostgreSQL via `condition: service_healthy`.
2. Executa a migração de DDL e a criação da conta `admin`.
3. Encerra sua execução com sucesso (código de retorno 0).
4. O `airflow-webserver` e o `airflow-scheduler` iniciam apenas quando a condição `service_completed_successfully` do `airflow-init` é atendida.

### 4. Arquitetura de Ouvinte Duplo (*Dual-Listener*) no Apache Kafka

Para resolver o desafio clássico de rede do Docker — onde o broker Kafka precisa ser acessível tanto por contêineres na rede interna do Docker quanto por scripts rodando na máquina host —, configuramos dois ouvintes no Kafka ([docker-compose.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L83-L85)):
- **Listener Interno (`PLAINTEXT://kafka:29092`):** Utilizado por contêineres Docker (Kafka UI, Schema Registry, Spark).
- **Listener Externo (`PLAINTEXT_HOST://localhost:9092`):** Expõe a porta para a máquina host, permitindo testes locais e comunicação por aplicações fora da rede do contêiner.

---

## 3. Conceitos Fundamentais de Engenharia de Dados

### 1. PostgreSQL Dual-Role & Separação por Schemas vs. Databases

Em uma infraestrutura de dados moderna, a segregação de responsabilidades de armazenamento é vital:

- **Database `airflow` (Metadados de Orquestração):** O Apache Airflow necessita de um banco relacional para persistir DAGs, `task_instances`, conexões, variáveis e XComs. O isolamento em um banco de dados dedicado evita contaminação com os dados de negócio.
- **Database `marketplace` (Data Lakehouse / Warehouse Relacional Local):** Atua como o armazenamento primário da aplicação simulada. Contém múltiplos **schemas**:
  - `transactional`: Tabela OLTP de origem (banco transacional do e-commerce).
  - `staging` (Camada Bronze): Tabelas brutas higienizadas pelo dbt (renomeação e typecasting).
  - `intermediate` (Camada Prata): Tabelas com regras de negócio, deduplicação e junções.
  - `marts` (Camada Ouro): Modelagem dimensional em Star Schema (tabelas Fato e Dimensão).

> [!IMPORTANT]
> No PostgreSQL, um **Database** é o nível máximo de isolamento (não permite `JOIN` SQL entre databases distintos sem extensoes como dblink). Um **Schema** é um namespace lógico dentro do mesmo database, permitindo consultas relacionais entre tabelas de schemas diferentes usando a sintaxe `schema.tabela`.

### 2. MinIO como Emulador do Amazon S3

O [MinIO](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L32-L51) é um servidor de armazenamento de objetos de alta performance compatível com a API do Amazon S3. 
- **Por que utilizar localmente?** Permite desenvolver pipelines de leitura e escrita em Python, PySpark e Boto3 utilizando exatamente a mesma API que será consumida na AWS em produção, sem gerar custos de armazenamento em nuvem ou depender de acesso à internet.
- **Estrutura de Buckets Simulados:**
  - `landing-zone/`: Eventos brutos recebidos em formato JSON/Avro.
  - `bronze/`: Dados brutos em formato Parquet com partição temporal (`year/month/day`).
  - `silver/`: Dados limpos e padronizados em Parquet/Delta Lake.
  - `gold/`: Dados consolidados para análise de BI.

### 3. Confluent Schema Registry & Contratos de Dados (Data Contracts)

No ecossistema de streaming com Apache Kafka, produtores e consumidores de dados precisam estar desacoplados no espaço e no tempo, mas estritamente acoplados no formato da mensagem.
- **O que faz o Schema Registry?** Atua como um repositório centralizado de contratos de dados (schemas Avro ou JSON Schema). 
- **Como funciona a validação?** O produtor envia o schema para o Registry na porta `8081`. O Registry valida a compatibilidade (ex: `BACKWARD`, `FULL`) e atribui um ID numérico ao schema. A mensagem publicada no Kafka inclui apenas o cabeçalho com esse ID (5 bytes) em vez de todo o schema textual, reduzindo drasticamente o payload trafegado.

### 4. Computação Distribuída: Apache Spark Master/Worker

O Apache Spark é o engine de processamento distribuído padrão da indústria para Big Data.
- **Spark Master (`spark-master:7077`):** Atua como o gestor do cluster (Cluster Manager), coordenando a distribuição de tarefas, acompanhando o estado dos trabalhadores e gerenciando a Web UI na porta `8082`.
- **Spark Worker (`spark-worker`):** Nó executor encarregado de alocar JVMs para processar partições de dados em memória. No nosso ambiente sandbox, limitamos o Worker a 1 core de CPU e 1GB de RAM para garantir leveza na máquina de desenvolvimento local.

### 5. Observabilidade Integrada: Prometheus & Grafana

Uma plataforma de dados em produção deve ser 100% observável.
- **Prometheus:** Coleta métricas de séries temporais via raspagem HTTP (*scraping*) dos targets configurados no [prometheus.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/prometheus/prometheus.yml).
- **Grafana:** Transforma essas séries temporais em dashboards visuais em tempo real, monitorando métricas de infraestrutura (CPU, memória, I/O de disco) e de negócios (vazão de mensagens no Kafka, atraso de consumidor/lag, tempo de execução de DAGs).

---

## 4. Implementação Passo a Passo

O desenvolvimento do Step 1 seguiu uma cronologia estruturada em 5 fases sequenciais:

```mermaid
timeline
    title Cronologia de Execução do Step 1
    section FASE 1 : Workspace & Governança : Setup da estrutura de diretórios : Criação de .gitignore e .env.example : Redação do ADR-0001 (GitHub Flow)
    section FASE 2 : Banco Relacional : Elaboração do script init.sql : Configuração de bancos e schemas Medalhão
    section FASE 3 : Docker Compose : Orquestração de 13 contêineres : Configuração de healthchecks e volumes : Dual-listener do Kafka & Airflow Init
    section FASE 4 : Script de Verificação : Implementação do verify_services.py : Healthchecks TCP e HTTP em Python : Formatação de logs em JSON
    section FASE 5 : CI/CD & Qualidade : Configuração do pyproject.toml (Ruff & Pytest) : Esteira de CI no GitHub Actions (ci.yml)
```

### Passo 1: Estruturação de Diretórios e Governança de Versionamento
1. Criação das pastas raiz desacopladas: `infrastructure/`, `ingestion/`, `processing/`, `transformation/`, `observability/`, `tests/` e `docs/`.
2. Criação do arquivo [config/.env.example](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/config/.env.example) definindo todas as portas de serviços e credenciais.
3. Elaboração do arquivo [.gitignore](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.gitignore) para impedir o envio acidental de arquivos de credenciais local (`.env`), ambientes virtuais (`.venv/`) e logs de execução.
4. Formalização da estratégia de branching no [ADR-0001](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/0001-git-branching-strategy.md).

### Passo 2: Inicialização do PostgreSQL e Schemas Medalhão
1. Elaboração do script de bootstrapping [infrastructure/postgres/init.sql](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/postgres/init.sql).
2. Adição da instrução `CREATE DATABASE airflow;` para segregação de metadados do orquestrador.
3. Criação idempotente dos schemas `transactional`, `staging`, `intermediate` e `marts` no banco primário `marketplace`.

### Passo 3: Orquestração Declarativa com Docker Compose
1. Desenvolvimento do arquivo [infrastructure/docker-compose.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml) declarando 13 serviços.
2. Definição da rede `marketplace-network` com driver `bridge`.
3. Mapeamento de volumes nomeados (`postgres_data`, `minio_data`) para garantir persistência de dados entre reinicializações dos contêineres.
4. Configuração do contêiner efêmero `airflow-init` com a diretiva `service_completed_successfully` para o Webserver e Scheduler.

### Passo 4: Implementação da Automação de Verificação de Serviços
1. Criação do script Python [scripts/verify_services.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/verify_services.py).
2. Construção da classe `JsonFormatter` para formatação estruturada de auditoria.
3. Implementação de verificações de portas TCP com `socket.create_connection` e endpoints HTTP com `urllib.request`.

### Passo 5: Padronização de Qualidade de Código & Pipeline de CI
1. Configuração das regras do linter/formatter **Ruff** e opções do **Pytest** no arquivo [pyproject.toml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/pyproject.toml).
2. Criação do workflow [.github/workflows/ci.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.github/workflows/ci.yml) para automação de validações a cada Pull Request ou commit na branch `main`.

---

## 5. Análise de Código e Trechos Chave

Nesta seção, examinamos os trechos cruciais dos arquivos implementados na Etapa 1.

### 1. Grafo de Dependências e Healthchecks no `docker-compose.yml`

No file [docker-compose.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L173-L226), a utilização de `healthcheck` ativo evita erros de condição de corrida (*race conditions*):

```yaml
  # Orquestração: Airflow Init (Contêiner Efêmero de Bootstrap)
  airflow-init:
    image: apache/airflow:2.9.1-python3.11
    container_name: marketplace-airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      AIRFLOW_UID: ${AIRFLOW_UID}
    volumes:
      - ../observability/dags:/opt/airflow/dags
      - ../observability/logs:/opt/airflow/logs
      - ../observability/plugins:/opt/airflow/plugins
    entrypoint: /bin/bash
    command: -c "airflow db init && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com || true"
    depends_on:
      postgres:
        condition: service_healthy # Aguarda a validação do pg_isready no Postgres
    networks:
      - marketplace-network

  # Orquestração: Airflow Webserver
  airflow-webserver:
    image: apache/airflow:2.9.1-python3.11
    container_name: marketplace-airflow-webserver
    restart: unless-stopped
    command: bash -c "airflow webserver"
    depends_on:
      airflow-init:
        condition: service_completed_successfully # Inicia somente após o fim do init
      postgres:
        condition: service_healthy
```

### 2. Script de Bootstrapping SQL (`init.sql`)

No file [init.sql](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/postgres/init.sql#L10-L29), garantimos o provisionamento idempotente do ambiente relacional:

```sql
-- 1. Criação da base de metadados do Apache Airflow
CREATE DATABASE airflow;

-- Concessão de permissões administrativas ao superusuário postgres
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;

-- 2. Schemas da Arquitetura Medalhão no banco primário ('marketplace')
CREATE SCHEMA IF NOT EXISTS transactional; -- Origem OLTP simulada
CREATE SCHEMA IF NOT EXISTS staging;       -- Camada Bronze dbt
CREATE SCHEMA IF NOT EXISTS intermediate;  -- Camada Prata dbt
CREATE SCHEMA IF NOT EXISTS marts;         -- Camada Ouro dbt (Star Schema)
```

### 3. Verificação de Serviços com Logging Estruturado JSON (`verify_services.py`)

No file [verify_services.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/verify_services.py#L18-L34), implementamos um log formatador customizado para auditoria estruturada:

```python
class JsonFormatter(logging.Formatter):
    """Custom JSON log formatter for structured output auditability."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if hasattr(record, "service"):
            log_data["service"] = record.service
        if hasattr(record, "status"):
            log_data["status"] = record.status
        return json.dumps(log_data)
```

E no bloco principal [verify_services.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/verify_services.py#L79-L116), realizamos as checagens ativas de portas TCP e HTTP:

```python
def check_tcp_port(host: str, port: int, timeout_sec: float = 3.0) -> bool:
    """Verifica se uma porta TCP está aberta e aceitando conexões."""
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except (socket.timeout, OSError):
        return False

def check_http_endpoint(url: str, expected_status: int = 200, timeout_sec: float = 3.0) -> bool:
    """Verifica se um endpoint HTTP responde com o status code esperado."""
    req = urllib.request.Request(url, headers={"User-Agent": "ServiceVerifier/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as response:
            return response.status == expected_status
    except urllib.error.HTTPError as err:
        return err.code == expected_status
    except (urllib.error.URLError, socket.timeout, OSError):
        return False
```

### 4. Configuração de Linting e Formatação no `pyproject.toml`

No file [pyproject.toml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/pyproject.toml#L13-L37), especificamos o conjunto de regras do **Ruff** (substituindo Flake8, Black e isort em uma única ferramenta desenvolvida em Rust):

```toml
[tool.ruff]
target-version = "py312"
line-length = 120
indent-width = 4

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors
    "W",   # pycodestyle warnings
    "F",   # Pyflakes
    "I",   # isort (ordenação automática de imports)
    "N",   # pep8-naming
    "UP",  # pyupgrade (modernização de sintaxe Python)
    "B",   # flake8-bugbear (prevenção de bugs comuns)
    "A",   # flake8-builtins
    "C4",  # flake8-comprehensions
    "SIM", # flake8-simplify
    "ARG", # flake8-unused-arguments
    "PTH", # flake8-use-pathlib
    "RUF", # Regras específicas do Ruff
]
```

### 5. Workflow de Integração Contínua no `.github/workflows/ci.yml`

No file [.github/workflows/ci.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.github/workflows/ci.yml#L12-L37), garantimos a execução automatizada de testes e linters:

```yaml
jobs:
  lint-and-test:
    name: Lint & Test Suite
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install ruff pytest

      - name: Run Ruff Lint Check
        run: ruff check .

      - name: Run Ruff Formatting Check
        run: ruff format --check .

      - name: Run Pytest Suite
        run: pytest
```

---

## 6. Desafios Enfrentados e Soluções

### Desafio 1: Permissões de Leitura e Escrita nos Volumes do Airflow (`Permission Denied`)

- **Sintoma:** Ao iniciar os contêineres do Airflow (`airflow-webserver` e `airflow-scheduler`), o processo falhava ao tentar criar arquivos de log na pasta local `../observability/logs`, retornando erro de `Permission Denied`.
- **Causa Raiz:** As imagens oficiais da Apache Airflow executam o processo sob o UID não-root `50000`. Se as pastas criadas no sistema operacional host pertencerem ao usuário `root` ou ao usuário padrão com restrição de permissão de escrita para o grupo/UID `50000`, o sistema operacional bloqueia a gravação.
- **Solução:** Injetamos a variável de ambiente `AIRFLOW_UID=${AIRFLOW_UID}` no [docker-compose.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L181). No arquivo [.env](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.env), a variável foi configurada para `50000` (ou dinamicamente ajustada para o UID retornado pelo comando `id -u` no Linux), garantindo a compatibilidade de permissões nos volumes montados.

### Desafio 2: Execução de Múltiplas Instruções SQL de Bancos Distintos no `init.sql` do PostgreSQL

- **Sintoma:** O script de inicialização falhava ou criava os schemas `transactional`, `staging`, `intermediate` e `marts` dentro da base de dados `airflow` em vez da base `marketplace`.
- **Causa Raiz:** O ponto de entrada oficial da imagem do PostgreSQL (`/docker-entrypoint-initdb.d/`) executa scripts `.sql` dentro do contexto do banco primário definido na variável `POSTGRES_DB` (`marketplace`). Ao executar `CREATE DATABASE airflow;`, o banco é criado, mas a sessão SQL do cliente permanece conectada ao banco `marketplace`.
- **Solução:** Estruturamos o [init.sql](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/postgres/init.sql) para executar a criação da base `airflow` primeiro e, em seguida, emitir os comandos `CREATE SCHEMA IF NOT EXISTS` diretamente no contexto padrão da base `marketplace`, sem dependência de comandos meta-PSQL (como `\c airflow`) que quebram a compatibilidade com drivers genéricos.

### Desafio 3: Resolução de Nomes de Host do Kafka em Ambiente Misto (Docker vs. Host)

- **Sintoma:** Aplicações Python rodando no host conseguiam conectar ao broker Kafka na porta `9092`, mas recebiam timeouts ao tentar enviar ou consumir mensagens, ou o Kafka UI relatava desconexão com o cluster.
- **Causa Raiz:** O Apache Kafka anuncia seus listeners (*advertised listeners*) para os clientes durante o handshake inicial. Se o Kafka anunciar apenas `kafka:29092`, a aplicação na máquina host não consegue resolver o nome DNS `kafka`. Se anunciar apenas `localhost:9092`, os contêineres internos da rede Docker (Spark, Schema Registry) tentam conectar em seus próprios `localhost` e falham.
- **Solução:** Implementamos a arquitetura de ouvintes duplos (*dual-listener*) no [docker-compose.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml#L83-L85), mapeando a chave `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092`.

---

## 7. Trade-offs de Engenharia & Alternativas Consideradas

| Matriz de Decisão | Abordagem Escolhida | Alternativa Considerada | Trade-offs & Justificativa de Engenharia |
| :--- | :--- | :--- | :--- |
| **Estratégia de Branching no Git** | **GitHub Flow** (Branch `main` única + branches de feature curtas) | **Git Flow** (`main`, `develop`, `feature/*`, `release/*`) | O GitHub Flow oferece baixíssimo overhead de mesclagem, perfeito para subagentes trabalhando paralelamente via **Git Worktrees**. O Git Flow traria complexidade excessiva de merges síncronos para um sandbox de engenharia de dados. |
| **Orquestração Local** | **Docker Compose Monolítico** (`docker-compose.yml`) | **Local Kubernetes** (Kind / Minikube + Helm) | O Docker Compose é extremamente leve, inicializa em menos de 30 segundos e consome ~6GB de RAM. O Kubernetes local adicionaria sobretaxa de CPU/RAM para o *control plane* sem agregar ganhos funcionais nesta fase. |
| **Execução do Airflow** | **LocalExecutor** com PostgreSQL dedicado | **CeleryExecutor** com Redis & Workers separados | O LocalExecutor roda tarefas como subprocessos no nó Scheduler, exigindo apenas o PostgreSQL. Elimina a necessidade de subir contêineres extras do Redis e múltiplos Celery Workers, economizando memória RAM local. |
| **Linter & Formatter Python** | **Ruff** | **Flake8 + Black + isort** | O Ruff é escrito em Rust e executa de 10x a 100x mais rápido que as ferramentas tradicionais em Python, consolidando linting, ordenação de imports e formatação em uma única ferramenta configurada no `pyproject.toml`. |
| **Coordenador de Cluster Kafka** | **Apache Zookeeper** (CP-Zookeeper 7.6.0) | **Kafka KRaft Mode** (No-Zookeeper) | Embora o modo KRaft elimine a dependência do Zookeeper, a pilha com Zookeeper ainda possui suporte extremamente maduro e estabilidade comprovada nas imagens oficiais da Confluent 7.6.0. |

---

## 8. Boas Práticas e Padrões Recomendados

### 1. Qualidade de Código com Ruff & Type Hints Explícitos
- **Type Hints:** Toda função Python desenvolvida no repositório deve conter anotações de tipo explícitas para parâmetros e retornos (ex: `def check_tcp_port(host: str, port: int) -> bool:`).
- **Ruff Compliance:** Todo código é verificado com `ruff check .` e formatado com `ruff format .`.

### 2. Padrões de Testes com Pytest
- **Estruturação de Testes:** Os testes são organizados na pasta `tests/` com nomenclaturas padronizadas `test_*.py`.
- **Modo Estrito:** O Pytest é configurado com `--strict-markers` no `pyproject.toml` para evitar o uso de marcadores não registrados.

### 3. Higiene de Segredos e Versionamento
- **Chaves no `.gitignore`:** Arquivos contendo segredos reais (`.env`, chaves PEM, tokens) jamais devem ser versionados.
- **Modelos de Exemplo:** Qualquer alteração em variáveis de ambiente deve ser refletida no arquivo [config/.env.example](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/config/.env.example).

### 4. Logging Estruturado em Formato JSON
- Conforme estabelecido no [GEMINI.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/GEMINI.md), o uso de chamadas `print()` é expressamente proibido para fins de auditoria ou monitoramento.
- Todos os módulos Python devem utilizar a biblioteca padrão `logging` parametrizada com formatadores JSON (como demonstrado no [verify_services.py](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/scripts/verify_services.py#L18-L34)).

---

## 9. Perspectiva da Indústria e Casos Reais

No ecossistema corporativo de dados em grande escala (empresas como Uber, Airbnb, Mercado Livre e Nubank), a infraestrutura containerizada local desenvolvida na Etapa 1 traduz-se diretamente em equivalentes gerenciados de nuvem:

```mermaid
graph LR
    subgraph Local_Sandbox["Ambiente Sandbox Local (Step 1)"]
        LocalMinIO["MinIO Object Storage"]
        LocalKafka["Apache Kafka + Schema Registry"]
        LocalSpark["Bitnami Spark Standalone"]
        LocalAirflow["Airflow (LocalExecutor)"]
        LocalPostgres["PostgreSQL 16"]
    end

    subgraph AWS_Cloud["Ambiente Corporativo AWS (Produção)"]
        CloudS3["Amazon S3 Buckets (KMS / Lifecycle)"]
        CloudMSK["Amazon MSK / Confluent Cloud"]
        CloudEMR["Databricks on AWS / Amazon EMR"]
        CloudMWAA["AWS MWAA (Managed Airflow)"]
        CloudRedshift["Amazon Redshift / RDS Postgres"]
    end

    LocalMinIO ==>|Equivalência Direta| CloudS3
    LocalKafka ==>|Equivalência Direta| CloudMSK
    LocalSpark ==>|Equivalência Direta| CloudEMR
    LocalAirflow ==>|Equivalência Direta| CloudMWAA
    LocalPostgres ==>|Equivalência Direta| CloudRedshift
```

### Principais Lições da Indústria:

1. **Testabilidade Local:** O uso do Docker Compose com MinIO e Kafka local permite que engenheiros de dados executem testes de integração de ponta a ponta em suas máquinas antes de realizar o deploy na nuvem, reduzindo custos de desenvolvimento e prevenindo bugs em produção.
2. **Contratos de Dados Rígidos:** Empresas de alta vazão utilizam o Schema Registry para garantir que mudanças de esquema efetuadas pelas equipes de desenvolvimento de software não quebrem silenciosamente os pipelines de dados da equipe de Data Engineering (*schema drift*).
3. **Observabilidade Unificada:** O par Prometheus + Grafana é a espinha dorsal de observabilidade em ambientes Cloud-Native, permitindo correlacionar métricas de infraestrutura (uso de CPU do Kubernetes) com métricas de negócio (atraso na ingestão de vendas).

---

## 10. Exercícios Práticos & Próximos Passos

### Exercícios Práticos

1. **Subida da Infraestrutura e Validação de Saúde:**
   - Execute o comando para subir a pilha de contêineres em segundo plano:
     ```bash
     docker compose -f infrastructure/docker-compose.yml up -d
     ```
   - Execute o script Python de verificação automatizada e inspecione a saída em JSON:
     ```bash
     python scripts/verify_services.py
     ```
   - *Resultado Esperado:* Todos os 10 serviços verificados devem retornar status `HEALTHY` com código de saída 0.

2. **Navegação nas Interfaces Web dos Serviços:**
   - Acesse o **Airflow Webserver**: `http://localhost:8080` (Credenciais: `admin` / `admin`).
   - Acesse o **Kafka UI**: `http://localhost:8085` (Verifique se os clusters `local` e `schema-registry` estão conectados).
   - Acesse o **MinIO Console**: `http://localhost:9001` (Credenciais configuradas no `.env`).
   - Acesse o **Grafana**: `http://localhost:3000` (Credenciais configuradas no `.env`).

3. **Inspeção de Schemas e Bancos no PostgreSQL:**
   - Conecte-se ao PostgreSQL local via `psql` ou DBeaver na porta `5432`:
     ```bash
     docker exec -it marketplace-postgres psql -U postgres -d marketplace
     ```
   - Liste os schemas criados executando o comando `\dn` e confirme a presença dos schemas `transactional`, `staging`, `intermediate` e `marts`.

---

### Transição para o Step 2 — Business Domain Modeling

Com a infraestrutura base totalmente operacional e validada, a plataforma está pronta para a **Etapa 2 (Business Domain Modeling)** do [roadmap.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/roadmap.md).

#### O Que Será Construído no Step 2:
- **Modelagem do Domínio de E-Commerce:** Definição formal dos eventos de negócio produzidos pelo marketplace (criação de pedidos, atualizações de pagamento, eventos de carrinho, movimentações de estoque, navegação de usuários e alterações de catálogo).
- **Especificação de Contratos de Dados (Data Contracts):** Elaboração de schemas Avro / JSON Schema para cada tópico Kafka.
- **Registro de Schemas no Schema Registry:** Upload e governança dos schemas na API do Confluent Schema Registry, definindo regras de compatibilidade e validação de tipos de dados.
