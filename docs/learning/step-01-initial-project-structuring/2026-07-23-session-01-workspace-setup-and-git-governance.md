# Sessão 01: Configuração do Workspace, Estratégia de Branching e Governança no Git

## 1. Resumo da Sessão

* **Tarefa(s) do Roadmap Concluída(s):**
  - Etapa 1 — Estruturação Inicial do Projeto (Fase 1: Configuração do Workspace, Variáveis de Ambiente e Governança de Versionamento)
* **Objetivos Alcançados:**
  - Criação da estrutura base de diretórios separando responsabilidades em `infrastructure/`, `ingestion/`, `processing/`, `transformation/`, `observability/`, `tests/` e `docs/`.
  - Formulação e documentação da estratégia de branching do projeto no [ADR-0001](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/0001-git-branching-strategy.md).
  - Parametrização de portas de serviços, credenciais de banco de dados e endpoints de storage via `config/.env.example` e `.env` local.
  - Configuração de regras estritas de ignore no Git (`.gitignore`) para evitar vazamento de credenciais e poluição do repositório.
* **Arquivos Modificados / Criados:**
  - [GEMINI.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/GEMINI.md)
  - [docs/adr/0001-git-branching-strategy.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/0001-git-branching-strategy.md)
  - [config/.env.example](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/config/.env.example)
  - [.env](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.env)
  - [.gitignore](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.gitignore)
  - [docs/context/current-step.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-step.md)
* **Principais Decisões Arquiteturais Tomadas:**
  - Adoção do **GitHub Flow** em detrimento do Git Flow para otimizar a execução paralela de subagentes usando Git Worktrees.
  - Implementação do padrão de desacoplamento de configurações de ambiente (Factor III do 12-Factor App) via arquivo `.env`.

---

## 2. O Que Foi Construído

Nesta sessão inicial, construímos a estrutura de diretórios e a configuração de ambiente fundamentais para uma plataforma de engenharia de dados corporativa.

```
Marketplace-Sandbox/
├── config/
│   └── .env.example             # Modelo contendo todas as variáveis de ambiente
├── docs/
│   ├── adr/                     # Registros de Decisões de Arquitetura (ADRs)
│   ├── context/                 # Estado do Roadmap, etapa atual e sessão ativa
│   └── learning/                # Documentação educacional & diário de aprendizado
├── infrastructure/              # Docker, Terraform, scripts de init do banco
├── ingestion/                   # Produtores Kafka em Python & simuladores de eventos
├── processing/                  # Scripts PySpark de processamento streaming
├── transformation/              # Modelos analíticos dbt (staging, intermediate, marts)
├── observability/               # DAGs Airflow, configurações de Prometheus & Grafana
├── tests/                       # Suites de testes unitários e de integração (Pytest)
├── .env                         # Credenciais de runtime locais & mapeamentos de portas
└── .gitignore                   # Regras de exclusão do controle de versão
```

### Visão Geral de Interação dos Componentes
A hierarquia de diretórios estabelece o isolamento de fronteiras para cada camada da pipeline de dados:
1. `ingestion/` abriga os geradores de eventos upstream (produtores).
2. `infrastructure/` mantém as orquestrações de contêineres locais (Kafka, Spark, MinIO, PostgreSQL, Airflow).
3. `processing/` consome tópicos Kafka via PySpark e grava os registros raw nas camadas Bronze e Silver do object storage.
4. `transformation/` executa os modelos dbt para transformar dados da camada Silver em modelos analíticos Gold em Star Schema.
5. `observability/` orquestra as tarefas via DAGs do Apache Airflow e monitora a vazão do sistema via Prometheus e Grafana.

---

## 3. Por Que Foi Construído Dessa Forma

### 1. Separação de Responsabilidades & Manutenibilidade
Misturar scripts de processamento, orquestração de contêineres e modelos analíticos em uma pasta única gera alto acoplamento. Ao impor uma separação de domínios no nível raiz (`ingestion/`, `processing/`, `transformation/`), os engenheiros podem atuar em camadas específicas sem causar impactos colaterais em componentes não relacionados.

### 2. Configuração no Padrão 12-Factor App (Fator III: Configurações)
Inserir URLs de banco de dados, endereços de bootstrap do Kafka ou chaves do MinIO diretamente nos scripts Python ou SQL introduz vulnerabilidades graves de segurança e quebra a portabilidade. Armazenar configurações em variáveis de ambiente (`.env`) injetadas em tempo de execução permite alternar perfeitamente entre contêineres Docker locais e ambientes de nuvem em produção (ex: AWS S3, Databricks, Redshift).

### 3. Estratégia de Branching Compatível com Git Worktree (GitHub Flow)
Modelos complexos de branching como o Git Flow envolvem branches `develop` de longa duração e múltiplos merges em estágios de release. Como os agentes de IA executam tarefas em paralelo usando Git Worktrees isolados (`git worktree add`), manter uma branch principal estável (`main`) com branches de feature de curta duração (`feature/*`) simplifica drasticamente operações de rebase e merge.

---

## 4. Conceitos Importantes

### Variáveis de Ambiente & O Padrão `.env`
* **Definição:** Pares chave-valor armazenados fora do código-fonte da aplicação, disponibilizados para os processos pelo sistema operacional ou runtime do contêiner.
* **Por que existe:** Protege credenciais e segredos (senhas, chaves de acesso) e desacopla o código da configuração do ambiente de destino.
* **Onde é usado:** Carregado pelo `docker-compose.yml`, scripts Python (`os.getenv()`) e perfis de conexão do dbt (`profiles.yml`).
* **Relação com a tarefa:** Configurado o `config/.env.example` como modelo versionado e o `.env` como arquivo local não versionado.

### Git Worktrees
* **Definição:** Recurso nativo do Git que permite ter múltiplos diretórios de trabalho associados a um único repositório, cada um apontando para uma branch diferente em pastas separadas.
* **Por que existe:** Elimina a necessidade de fazer `stash` de alterações não salvas ou mudar frequentemente de branch via `git checkout` ao alternar contextos.
* **Onde é usado:** Utilizado pelos subagentes (`invoke_subagent` no modo `share` ou `branch`) para executar tarefas em paralelo sem interferir no workspace principal.

### Architecture Decision Records (ADRs)
* **Definição:** Documentos leves em Markdown que registram uma decisão de arquitetura, seu contexto, direcionadores, opções consideradas e consequências.
* **Por que existe:** Evita a perda de conhecimento histórico e a "deriva arquitetural" ao longo do tempo, documentando a *justificativa* por trás das escolhas.
* **Onde é usado:** Criado o [ADR-0001](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/0001-git-branching-strategy.md) para formalizar a escolha do GitHub Flow.

---

## 5. Abordagens Alternativas

### Comparação: Modelos de Branching

| Critério | Git Flow | Trunk-Based Development | GitHub Flow (Escolhido) |
| :--- | :--- | :--- | :--- |
| **Estrutura** | `main`, `develop`, `feature/*`, `release/*`, `hotfix/*` | Branch `main` única com commits diretos e frequentes | Branch `main` estável + branches `feature/*` curtas |
| **Complexidade** | Alta (múltiplas branches de longa duração e merges) | Baixa (exige altíssima maturidade de integração contínua) | Média-Baixa (fluxo simples de PR e merge) |
| **Adequação a Worktrees** | Ruim (propenso a conflitos frequentes na `develop`) | Moderada (exige testes automatizados instantâneos) | **Excelente** (branches de feature isoladas por tarefa) |
| **Por que foi escolhido** | Rejeitado devido ao overhead excessivo de merges para os agentes. | Avaliado, mas branches de feature oferecem melhor isolamento para validação. | **Selecionado** pela simplicidade e compatibilidade com worktrees. |

---

## 6. Trade-offs

### 1. Simplicidade vs. Controle Granular de Releases
* **Trade-off:** Escolher o GitHub Flow em vez do Git Flow simplifica o fluxo dos agentes, mas abre mão de ambientes de staging/homologação dedicados (`release/*`).
* **Racional:** Em um sandbox educacional de engenharia de dados, o overhead de ambientes de staging é desnecessário. Testes de integração contínua na branch `main` fornecem a segurança necessária.

### 2. Separação de Diretórios vs. Overhead de Navegação
* **Trade-off:** Dividir o código em `ingestion/`, `processing/` e `transformation/` exige navegar por múltiplos diretórios em comparação a uma estrutura monolítica simples.
* **Racional:** A modularidade e a escalabilidade superam a pequena inconveniência de navegação.

---

## 7. Perspectiva de Mercado

* **Data Lakes e Data Warehouses Corporativos:** Em ambientes produtivos (ex: Netflix, Uber, Airbnb), repositórios de dados costumam seguir o modelo **monorepo** ou repositórios modulares por domínio. A infraestrutura (Terraform) é frequentemente gerenciada em repositórios separados com rígido controle de acesso IAM.
* **Gerenciamento de Segredos:** Enquanto o desenvolvimento local utiliza arquivos `.env`, plataformas produtivas utilizam gerenciadores dedicados como **HashiCorp Vault**, **AWS Secrets Manager** ou **GCP Secret Manager** com rotação automática de chaves e governança por papéis (RBAC).

---

## 8. Erros Comuns

1. **Commitar o arquivo `.env` no Controle de Versão:**
   - *Por que ocorre:* Desenvolvedores esquecem de adicionar o `.env` ao `.gitignore` antes de executar `git add .`.
   - *Como evitar:* Sempre crie o `.gitignore` e o `.env.example` primeiro, ignorando explicitamente o `.env`.
2. **Uso de Caminhos de Arquivo Relativos Estáticos (`../../arquivo.json`):**
   - *Por que ocorre:* Assumir que a execução sempre iniciará a partir de um diretório específico.
   - *Como evitar:* Calcule os caminhos com base na raiz do projeto programaticamente ou via variáveis de ambiente.

---

## 9. Leituras Recomendadas

* [The Twelve-Factor App — III. Configurações](https://12factor.net/pt_br/config)
* [Especificação do GitHub Flow](https://docs.github.com/pt/get-started/using-git/github-flow)
* [Documentação Oficial do Git Worktree](https://git-scm.com/docs/git-worktree/pt_br)

---

## 10. Principais Aprendizados

- **Bases Sólidas:** Uma estrutura de diretórios bem organizada e o padrão de variáveis de ambiente evitam débitos técnicos precoces na plataforma.
- **Alinhamento no Git:** Alinhar o modelo de branching (GitHub Flow) aos mecanismos de execução (Git Worktrees) reduz conflitos entre agentes automatizados.
- **Arquitetura Desacoplada:** Separar ingestão, processamento, transformação e observabilidade possibilita um desenvolvimento modular e escalável.
