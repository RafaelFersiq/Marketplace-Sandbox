# Recomendações de Skills — Marketplace Sandbox

Este documento apresenta uma análise detalhada das **skills** encontradas no repositório que são altamente relevantes para o desenvolvimento e o sucesso do projeto **Marketplace Sandbox**. As skills recomendadas foram mapeadas com base nas necessidades técnicas descritas em [scope.md](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/scope.md) e nos passos cronológicos definidos em [roadmap.md](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/roadmap.md).

---

## 1. Desenvolvimento, Ingestão & Tratamento de Erros (Passos 1, 3 e 4)

### 🐍 [python-pro](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/python-pro/SKILL.md)
* **Utilidade:** Desenvolvimento moderno em Python 3.12+, adoção de melhores práticas de formatação/linting com `ruff`, gerenciamento ultrarrápido de dependências com `uv` e criação de código altamente tipado e performático.
* **Por que será útil no projeto:**
  * **Passo 1 (Estrutura Inicial):** Configuração de ferramentas modernas de qualidade de código (linting e testes rápidos de build) no ambiente local.
  * **Passo 3 (Event Producers):** Criação dos simuladores de checkout, pagamentos, navegação, inventário e antifraude. A skill ajuda a projetar as classes dos simuladores com tipagem explícita e tratamento robusto de erros.
  * **Passo 4 (Ingestion Pipeline):** Suporte na elaboração de funções auxiliares robustas em Python para processamento de payloads antes e durante o consumo no Spark.

### ⚡ [async-python-patterns](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/async-python-patterns/SKILL.md)
* **Utilidade:** Domínio de programação concorrente em Python com `asyncio`, manipulação de event loops, concorrência baseada em tarefas e controle de chamadas assíncronas I/O-bound sem bloqueio.
* **Por que será útil no projeto:**
  * **Passo 3 (Event Producers):** Os simuladores de eventos em Python precisam emular o comportamento concorrente de múltiplos sistemas independentes (checkout, pagamentos, inventário). Esta skill é essencial para criar produtores assíncronos capazes de simular rajadas de tráfego (traffic bursts) de forma eficiente em um único processo sem sobrecarregar a CPU.

### ⚡ [spark-optimization](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/spark-optimization/SKILL.md)
* **Utilidade:** Otimização profunda de processamento de dados usando Apache Spark, abordando estratégias de particionamento (repartition vs coalesce), mitigação de data skew (salting), otimização de shuffles, joins de alta performance (como broadcast joins) e gerenciamento/tuning de memória.
* **Por que será útil no projeto:**
  * **Passo 4 (Streaming Ingestion Pipeline):** O Spark Structured Streaming requer parametrização correta de checkpoints e tamanho de partições para evitar latência e gargalos de rede ao ler tópicos do Kafka.
  * **Passos 5 e 6 (Bronze e Silver Layers):** Ajuda no particionamento temporal eficiente dos dados salvos no MinIO/S3 (camada Bronze) e na deduplicação e limpeza em massa no Spark (camada Silver) sem esgotar a memória dos executores.
  * **Passo 13 (Deploy AWS/Databricks):** Indispensável para ajustar as configurações de execução ao migrar a infraestrutura do Docker local para clusters gerenciados do Databricks na nuvem.

### ⚙️ [error-handling-patterns](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/error-handling-patterns/SKILL.md)
* **Utilidade:** Implementação de padrões robustos de tratamento de exceções, propagação de erros, mecanismos de retentativa (retries), circuit breakers e degradação suave (graceful degradation) para construir aplicações resilientes.
* **Por que será útil no projeto:**
  * **Passo 3 (Event Producers):** Os simuladores de eventos em Python interagem com o Kafka. Esta skill ajuda a lidar com falhas de rede intermitentes, aplicando retentativas exponenciais com jitter ao enviar mensagens.
  * **Passo 4 (Ingestion Pipeline):** Crucial para o Spark Structured Streaming ao deserializar mensagens Avro/JSON; ensina como isolar registros inválidos e enviá-los de forma resiliente para uma Dead Letter Queue (DLQ) ou área de quarentena sem interromper o processamento em tempo real.
  * **Passo 9 (Complete Orchestration):** Auxilia no design de DAGs do Airflow com caminhos alternativos em caso de falha de tarefas críticas.

---

## 2. Armazenamento, Modelagem & Otimização SQL (Passos 1, 7 e 8)

### 🐘 [postgresql](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/postgresql/SKILL.md)
* **Utilidade:** Design avançado de esquemas no PostgreSQL, seleção ideal de tipos de dados (como `TIMESTAMPTZ` para eventos e `BIGINT IDENTITY` para IDs), definição de restrições (constraints), indexação eficiente e estratégias de particionamento físico.
* **Por que será útil no projeto:**
  * **Passo 1 (Structuring):** Configuração correta do container PostgreSQL para atuar localmente tanto no repositório de metadados do Airflow quanto na simulação do banco de dados relacional de origem do marketplace.
  * **Passos 7 e 8 (dbt & Dimensional Modeling):** Essencial para projetar as tabelas físicas no PostgreSQL local de forma otimizada (como indexação manual de chaves estrangeiras - FKs, o que o PG não faz automaticamente) antes da migração final para o Redshift.

### 💎 [sql-pro](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/sql-pro/SKILL.md)
* **Utilidade:** Domínio de SQL moderno e analítico, incluindo Window Functions complexas, Common Table Expressions (CTEs) recursivas, manipulação avançada de dados semiestruturados (como JSONB/Arrays) e modelagem dimensional em warehouses (Redshift, BigQuery, Snowflake).
* **Por que será útil no projeto:**
  * **Passo 8 (Dimensional Modeling - Gold Layer):** Auxilia a escrever as queries analíticas que consolidam os KPIs de negócios (GMV, ticket médio, taxa de conversão e abandono de carrinho) utilizando queries performáticas e modernas em SQL/dbt.
  * **Passo 6 (Silver Layer):** Útil para a extração de atributos opcionais ou dinâmicos salvos em colunas JSONB, mapeando-os para colunas estruturadas na camada analítica.

### ⚡ [sql-optimization-patterns](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/sql-optimization-patterns/SKILL.md)
* **Utilidade:** Análise de planos de execução (`EXPLAIN`), eliminação de queries lentas, estratégias de indexação focadas no padrão de consulta e otimização de joins em grandes volumes de dados.
* **Por que será útil no projeto:**
  * **Passo 8 (Gold Layer):** Permite avaliar o plano de consulta das Fact e Dimension tables no Redshift/PostgreSQL, eliminando gargalos de joins pesados (evitando N+1 queries ou scans completos de tabelas).
  * **Passo 13 (AWS Deploy):** Essencial para otimizar os custos e performance de leitura no AWS Redshift quando o volume de dados simulado atingir níveis elevados.

### 📊 [dbt-transformation-patterns](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/dbt-transformation-patterns/SKILL.md)
* **Utilidade:** Práticas recomendadas em Analytics Engineering usando dbt Core, organização de camadas (`staging`, `intermediate` e `marts`), materialização incremental e documentação.
* **Por que será útil no projeto:**
  * **Passo 7 (dbt Setup):** Direciona a criação correta da estrutura do projeto dbt, definindo as fontes de dados e configurando a camada de `staging` e limpeza inicial.
  * **Passo 8 (Dimensional Modeling & Lineage):** Ensina como modelar as tabelas Dimensão e Fato (Star Schema) na camada de marts e a configurar os metadados para exportação do OpenLineage, gerando uma linhagem clara e auditável até o Redshift.

---

## 3. Orquestração, Qualidade & Governança (Passos 6, 9 e 10)

### ⚙️ [airflow-dag-patterns](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/airflow-dag-patterns/SKILL.md)
* **Utilidade:** Criação de DAGs de produção estruturadas segundo padrões de idempotência, tratamento correto de falhas e políticas de retentativas (retries), uso adequado de operadores e sensores, e otimização de concorrência.
* **Por que será útil no projeto:**
  * **Passo 9 (Complete Orchestration):** O Airflow será o cérebro da plataforma, coordenando a ingestão de dados, as transformações dbt e as tarefas de validação de qualidade. Essa skill fornece templates de DAGs resilientes para garantir o cumprimento de SLAs e evitar a execução desnecessária ou duplicada de tarefas.

### 🛡️ [data-quality-frameworks](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/data-quality-frameworks/SKILL.md)
* **Utilidade:** Validação de qualidade de dados de ponta a ponta, usando conceitos de contratos de dados (Data Contracts), testes dbt avançados e frameworks como Great Expectations para detecção de anomalias e integridade de dados.
* **Por que será útil no projeto:**
  * **Passo 2 (Business Domain Modeling):** Útil para a definição e documentação de regras de validação estrutural no Kafka Schema Registry.
  * **Passo 6 (Silver Layer):** Fornece abordagens para lidar com registros corrompidos e encaminhá-los para áreas de quarentena.
  * **Passo 10 (Data Quality Layer):** Crucial para a concepção do framework de qualidade contínua de dados no pipeline, garantindo a completude, consistência e frescor antes do carregamento final na Gold Layer.

### 🛡️ [gdpr-data-handling](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/gdpr-data-handling/SKILL.md)
* **Utilidade:** Definição de controles de privacidade de dados, anonimização, pseudonimização, políticas de ciclo de vida e conformidade regulatória para dados pessoais sensíveis (PII).
* **Por que será útil no projeto:**
  * **Passo 6 (Silver Layer):** O roadmap exige explicitamente regras de anonimização e conformidade de dados sensíveis (PII) durante a carga do Data Lakehouse. Esta skill guiará na aplicação de regras de mascaramento e criptografia nos dados de clientes (orders/navigation) no Spark.

### ⏱️ [slo-implementation](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/slo-implementation/SKILL.md)
* **Utilidade:** Definição e monitoramento de Indicadores de Nível de Serviço (SLIs) e Objetivos de Nível de Serviço (SLOs) com políticas de orçamento de erro (error budgets) e alertas baseados em taxa de consumo (burn rates).
* **Por que será útil no projeto:**
  * **Passo 9 (Complete Orchestration):** O roadmap pede a definição de SLAs operacionais na esteira de dados. Esta skill ajuda a estabelecer limites claros de sucesso operacional (como: "99% dos dados de transações devem ser orquestrados e disponibilizados na Gold layer em até 30 minutos") e mapeá-los para alertas no Airflow.

---

## 4. Infraestrutura, Segurança & Observabilidade (Passos 11, 12 e 13)

### ☁️ [terraform-specialist](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/terraform-specialist/SKILL.md)
* **Utilidade:** Domínio completo sobre Terraform e OpenTofu, com foco em desenvolvimento de módulos reutilizáveis, boas práticas de segurança, isolamento de ambientes e controle seguro de estado remoto (remote state e locks).
* **Por que será útil no projeto:**
  * **Passo 12 (Infrastructure as Code):** A automação de toda a infraestrutura exige uma modelagem modular limpa. A skill guiará na modularização de recursos locais (como redes Docker) e recursos de nuvem de maneira segura.
  * **Passo 13 (Cloud Deployment):** Imprescindível para provisionar a infraestrutura de rede segura (VPC, Subnets) e os serviços gerenciados na AWS (AWS Lambda, ECS, Amazon S3, AWS Redshift e AWS MWAA/Airflow) sem configurações manuais sujeitas a erros.

### 📈 [prometheus-configuration](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/prometheus-configuration/SKILL.md)
* **Utilidade:** Configuração detalhada do Prometheus para raspagem (scraping) de métricas de infraestrutura e aplicações, Service Discovery, criação de regras de gravação (recording rules) e regras de alerta.
* **Por que será útil no projeto:**
  * **Passo 11 (Observability and Monitoring):** Complementa o Grafana. Ensina a configurar o `prometheus.yml` para coletar métricas de performance do Kafka (lag de tópicos), Spark (tempo de execução de estágios), PostgreSQL e containers do Docker, definindo as regras que disparam alertas em caso de anomalias operacionais.

### 📉 [grafana-dashboards](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/grafana-dashboards/SKILL.md)
* **Utilidade:** Criação e provisionamento de painéis (dashboards) no Grafana para visualização em tempo real de métricas, integração com Prometheus e estruturação de observabilidade de serviços.
* **Por que será útil no projeto:**
  * **Passo 11 (Observability and Monitoring):** Ensina como plotar taxas de requests, latências de processamento (P95/P99) e tempos de resposta usando os métodos RED (para serviços e consumidores Kafka) e USE (para a infraestrutura Docker/AWS), criando alertas automatizados integrados com Prometheus.

### 🚀 [github-actions-templates](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/github-actions-templates/SKILL.md)
* **Utilidade:** Criação de pipelines automatizados de CI/CD rápidos e eficientes para validação de código, linting, testes unitários, builds de imagens Docker e deploy automático em ambientes de nuvem.
* **Por que será útil no projeto:**
  * **Passo 1 (Project Structuring):** Estrutura inicial do pipeline de validação local (com testes de sintaxe e estilo).
  * **Passo 13 (AWS Deploy & CI/CD):** Automatização do deploy da infraestrutura Terraform (`terraform plan`/`apply`), deploy automático de DAGs no AWS MWAA/ECS, e sincronização dos arquivos dbt, garantindo que qualquer alteração no repositório seja refletida de forma segura no ambiente em produção na AWS.

### 🔑 [secrets-management](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/secrets-management/SKILL.md)
* **Utilidade:** Implementação de armazenamento, rotação e recuperação segura de credenciais sensíveis (chaves de API, senhas de banco de dados, certificados TLS) usando ferramentas como AWS Secrets Manager, HashiCorp Vault e segredos nativos de CI/CD.
* **Por que será útil no projeto:**
  * **Passo 1 (Structuring):** Configuração de variáveis de ambiente seguras no Docker Compose local sem expor credenciais em arquivos versionados.
  * **Passo 13 (AWS Deploy & CI/CD):** Integração crítica no pipeline do GitHub Actions para recuperar de forma segura credenciais da AWS, senhas do Redshift e tokens do Databricks via AWS Secrets Manager ou GitHub Secrets durante os deploys de IaC com Terraform.

---

## 5. Governança Técnica & Documentação (Passos 1 e 14)

### 🔀 [git-advanced-workflows](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/git-advanced-workflows/SKILL.md)
* **Utilidade:** Domínio de fluxos avançados do Git (como rebase interativo, autosquash, worktrees e cherry-picking) para a preservação de um histórico linear, limpo e profissional de commits.
* **Por que será útil no projeto:**
  * **Passo 1 (Structuring):** Auxilia na implementação da estratégia de branching (dev/staging/main). Garante que commits temporários e correções de sintaxe de desenvolvimento local sejam limpos (`squashed` ou `reworded`) antes de abrir Pull Requests, permitindo que recrutadores e outros desenvolvedores naveguem facilmente pela evolução do projeto.

### 📝 [architecture-decision-records](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/architecture-decision-records/SKILL.md)
* **Utilidade:** Criação e manutenção de Architecture Decision Records (ADRs) utilizando templates padrão (como o MADR) para documentar o contexto, justificativas técnicas, trade-offs e consequências de decisões de arquitetura.
* **Por que será útil no projeto:**
  * **Passo 1 (Git & Structuring):** Estruturação do repositório contendo uma pasta `docs/adr` para registrar as decisões críticas.
  * **Passo 14 (Technical Documentation):** Permite justificar profissionalmente as escolhas de stack para recrutadores no portfólio (ex: por que usar PySpark em vez de dbt puro para a camada Silver, ou por que migrar do MinIO para S3). Transforma o projeto em um estudo de caso realista de Engenharia de Software/Dados.

### 📖 [docs-architect](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/docs-architect/SKILL.md)
* **Utilidade:** Criação de manuais técnicos de alta qualidade, guias de arquitetura e documentações de fluxo de dados de ponta a ponta a partir da análise da base de código.
* **Por que será útil no projeto:**
  * **Passo 14 (Technical Documentation and Portfolio):** A principal entrega de um projeto de portfólio é a clareza da documentação. Esta skill guia na criação do README principal, diagramas de fluxo de dados (Data Flow Diagrams), mapeamento de APIs/eventos e guias operacionais que demonstram domínio completo sobre a arquitetura do "Marketplace Sandbox".

### 📊 [mermaid-expert](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/mermaid-expert/SKILL.md)
* **Utilidade:** Criação e estilização de diagramas em texto (flowcharts, sequence diagrams, ERDs, Gantt) integrados nativamente em Markdown.
* **Por que será útil no projeto:**
  * **Passo 14 (Technical Documentation):** Permite construir diagramas visuais vivos de linhagem de dados e fluxos de tarefas diretamente em arquivos `.md` do repositório (ex: diagramas de fluxo de dados do Kafka ao Redshift), facilitando revisões de código e documentação evolutiva sem depender de ferramentas de design externas.

### 🏗️ [c4-architecture-c4-architecture](file:///home/rafael-siqueira/Downloads/antigravity-skills-main/skills/c4-architecture-c4-architecture/SKILL.md)
* **Utilidade:** Estruturação de diagramas de arquitetura de software divididos em 4 níveis (Contexto, Container, Componente, Código) para simplificar a explicação de sistemas complexos a diferentes públicos.
* **Por que será útil no projeto:**
  * **Passo 14 (Portfolio):** A arquitetura do "Marketplace Sandbox" envolve muitos serviços integrados (Docker, Kafka, Spark, Postgres, MinIO, Airflow, dbt, Grafana). O padrão C4 permite criar diagramas de Contexto (pessoas e fluxos macro) e de Container (tecnologias e conexões) padronizados, facilitando o entendimento de recrutadores e arquitetos de dados que avaliarem o seu repositório.
