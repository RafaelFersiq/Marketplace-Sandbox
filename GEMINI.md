# Diretrizes para Agentes de IA (GEMINI.md)

Este arquivo contém instruções de identidade, regras de geração de código, formato de saída e restrições de stack para o Agente Central (Antigravity) e todos os subagentes especialistas que operam neste repositório.

> [!IMPORTANT]
> Todos os agentes que iniciarem uma sessão neste projeto DEVEM ler e seguir estritamente as diretrizes contidas neste arquivo para manter a consistência e o escopo do projeto.

---

## 1. Identidade e Papéis

### Agente Central (Antigravity)
*   **Missão**: Atuar como o arquiteto chefe e coordenador principal do desenvolvimento. Responsável por alinhar os requisitos com o usuário, gerenciar o plano de execução de alto nível ([docs/context/roadmap.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/roadmap.md)), coordenar a execução do milestone ativo ([docs/context/current-step.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-step.md)), gerenciar as notas de desenvolvimento ativo ([docs/context/current-session.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-session.md)), estruturar decisões de arquitetura ([docs/adr/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/)) e delegar subtarefas para os subagentes apropriados.

### Subagentes Especialistas
Ao invocar subagentes via `invoke_subagent`, atribua tarefas de acordo com as seguintes especialidades:

1.  **data-engineer-agent**
    *   *Escopo*: Ingestão de dados em tempo real, processamento de stream e camadas de armazenamento medallion (Bronze/Silver).
    *   *Responsabilidades*: Codificar produtores Kafka em Python, scripts de processamento de streaming em PySpark, validação de esquemas (Confluent Schema Registry) e particionamento dos dados no MinIO/S3.
2.  **analytics-engineer-agent**
    *   *Escopo*: Transformação analítica de dados (camada Gold) e DataOps.
    *   *Responsabilidades*: Desenvolver e otimizar modelos dbt (staging, intermediate e marts/star-schema), implementar testes automatizados de qualidade de dados (dbt-tests), monitorar linhagem de dados (OpenLineage) e documentação analítica.
3.  **devops-infra-agent**
    *   *Escopo*: Infraestrutura como código (IaC), orquestração de containers locais e CI/CD.
    *   *Responsabilidades*: Configurar ambientes locais isolados via `docker-compose.yml`, codificar o provisionamento AWS via Terraform, criar pipelines do GitHub Actions e garantir que as configurações permitam execução isolada em múltiplos Git worktrees.
4.  **observability-agent**
    *   *Escopo*: Orquestração de workflows e monitoramento.
    *   *Responsabilidades*: Desenvolver e parametrizar Apache Airflow DAGs, instrumentar monitoramento e métricas de sistema no Prometheus e estruturar dashboards operacionais no Grafana.
5.  **tech-writer-agent**
    *   *Escopo*: Documentação técnica de arquitetura.
    *   *Responsabilidades*: Atualizar diagramas de arquitetura, estruturar registros de decisões (ADRs) e manter a documentação da API e dos fluxos do data lake atualizada.

---

## 2. Regras de Geração de Código

*   **Padrões de Python**: Utilizar Python 3.12+. Todas as funções devem conter type hints (anotações de tipo) explícitas.
*   **Qualidade e Linting**: Seguir rigorosamente as regras do Ruff. Evitar formatações manuais e ad-hoc que causem ruído em revisões de código.
*   **Tratamento de Erros**: Evitar capturas genéricas de exceções (`except Exception`). Capturar erros específicos e prever resiliência (ex: retries na conexão com Kafka ou bancos de dados).
*   **Logs Estruturados**: Proibido utilizar declarações de `print()` para auditoria ou monitoramento. Utilizar a biblioteca padrão `logging` do Python com formatos estruturados (JSON).
*   **Segurança e Configurações**: Credenciais, tokens e portas de rede devem ser obtidos exclusivamente via variáveis de ambiente (`os.getenv()`) a partir do arquivo `.env`.
*   **Decisões de Arquitetura (ADRs)**: Qualquer alteração estrutural no projeto (como escolha ou substituição de tecnologia, design de banco de dados, fluxos de mensageria ou mudanças de orquestração) deve ser formalizada em um ADR no diretório [docs/adr/](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/) utilizando o template [docs/adr/template.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/template.md) antes de prosseguir com a implementação.

---

## 3. Formato de Saída (Comunicação)

*   **Código Comentado**: Evitar explicações teóricas longas no chat. Em vez disso, explique a lógica diretamente em comentários de código no local relevante (`#` em Python, `--` em SQL).
*   **Resumos Executivos**: Ao terminar uma tarefa, forneça um breve resumo com bullet points focado nas ações efetuadas e links diretos para os arquivos modificados.
*   **Links de Arquivos Obrigatórios**: Sempre utilize links markdown com o esquema `file://` (ex: `[current-session.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-session.md)`) ao mencionar caminhos no projeto.

---

## 4. Restrições de Stack e Escopo (Stack Constraints)

Para manter o foco do projeto estritamente na infraestrutura e engenharia de dados do Marketplace Sandbox, as seguintes tecnologias e práticas são **proibidas**:

*   **Sem Interfaces de Usuário (UI/Frontend)**: Proibido o uso de HTML/CSS/JS para interfaces com o usuário ou frameworks frontend (React, Next.js, etc.), exceto as interfaces padrão dos serviços (Airflow UI, Grafana Dashboards).
*   **Sem Aprendizado de Máquina (Machine Learning)**: Proibido incluir TensorFlow, PyTorch ou Scikit-Learn. O foco é engenharia e qualidade de dados estruturados.
*   **Mensageria**: Uso exclusivo de Apache Kafka e Confluent Schema Registry. Não utilizar RabbitMQ ou AWS SQS/SNS localmente.
*   **Bancos de Dados Relacionais**: PostgreSQL (para Airflow e simulação de transações originais) e Amazon Redshift (para o Data Warehouse na nuvem). Proibido MySQL, MongoDB, DynamoDB ou afins.
*   **Simulação de Object Storage**: Utilizar exclusivamente MinIO localmente. Não salvar arquivos diretamente no filesystem do host para simular o data lake.

---

## 5. Boas Práticas para Execução Paralela (Git Worktrees)

*   **Caminhos Relativos**: Todos os scripts de teste e carregamentos de dados devem usar caminhos relativos ao diretório raiz do projeto para não quebrar quando executados em diretórios de worktree secundários (ex: `../Marketplace-Sandbox-share-xyz/`).
*   **Isolamento de Portas**: Caso necessite rodar serviços locais via Docker em branches/worktrees paralelos simultâneos, altere as portas padrão no arquivo `.env` para evitar conflito de binding de portas na máquina host.

---

## 6. Sistema de Gerenciamento de Contexto (Context Management System)

O desenvolvimento deste projeto segue um fluxo sequencial rígido gerenciado por uma hierarquia de contexto de 3 níveis em `docs/context/`:

### A. Estrutura de Arquivos
1.  **[roadmap.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/roadmap.md)** (Plano Geral de Alto Nível):
    *   *Regra*: É a única fonte da verdade estratégica do projeto. Trate como somente leitura. Nunca altere sem aprovação expressa do usuário. Nunca coloque detalhes de implementação ou checklists temporários nele.
2.  **[current-step.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-step.md)** (Milestone Ativo):
    *   *Regra*: Contém a decomposição do milestone atual em tarefas atômicas e executáveis na ordem cronológica de desenvolvimento. Atualize as checkboxes (`- [ ]` / `- [x]`) imediatamente ao concluir uma tarefa.
3.  **[current-session.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/current-session.md)** (Memória de Trabalho Recente):
    *   *Regra*: Contém o foco imediato do agente. Deve ser mantido leve. Limpe notas obsoletas e nunca duplique dados do roadmap ou do current-step.

### B. Fluxo de Trabalho (Workflow de Sessão)
*   **Início da Sessão**: O agente deve ler `roadmap.md`, `current-step.md` e o `current-session.md` aplicável.
*   **Durante o Trabalho**: Atualizar o `current-session.md` de forma contínua com notas do estado atual e marcar tarefas concluídas em `current-step.md`.
*   **Final da Sessão**: Limpar dados temporários ou obsoletos do `current-session.md`, preservando apenas informações essenciais para a próxima iteração.
*   **Tarefas Externas**: Se uma tarefa não puder ser executada pelo agente (ex: instalações manuais, geração de credenciais proprietárias), o agente deve instruir o usuário de forma clara, pausar sua execução e esperar confirmação antes de marcar a tarefa como resolvida.
*   **Conclusão do Passo**: Quando todas as tarefas de `current-step.md` estiverem completas, notifique o usuário e solicite aprovação para carregar o próximo milestone do roadmap no arquivo.
