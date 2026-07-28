# ADR-0004: Estratégia e Segregação do PostgreSQL Dual-Role

## Status

Accepted

## Contexto e Declaração do Problema

No ecossistema do Marketplace Sandbox, a base de dados relacional PostgreSQL assume dois papéis distintos e críticos:
1. **Banco de Metadados do Orquestrador (Airflow Metadata Store)**: Armazena estados de execução de DAGs, tarefas, variáveis, conexões e históricos de execução do Apache Airflow (`airflow`).
2. **Simulação Transacional de Aplicação e Schemas Medallion (Marketplace Application DB)**: Simula o banco transacional relacional OLTP do marketplace e armazena os schemas relacionais das camadas Staging, Intermediate e Marts da arquitetura Medallion (`marketplace`).

Precisamos definir a arquitetura de banco de dados para suportar esses dois papéis simultaneamente, garantindo isolamento estrito de dados, automação no bootstrap de inicialização e baixo consumo de recursos computacionais locais.

## Direcionadores da Decisão (Decision Drivers)

*   **Isolamento Lógico e de Segurança**: Impedir interferência cruzada entre metadados operacionais do Airflow e dados analíticos/transacionais da aplicação Marketplace.
*   **Eficiência de Recursos Locais**: Evitar a sobrecarga de memória e CPU resultante de rodar múltiplas instâncias/containers de PostgreSQL no ambiente de desenvolvimento local.
*   **Automação do Bootstrapping**: Garantir que bancos de dados, privilégios e schemas sejam provisionados automaticamente no momento em que a stack containerizada é iniciada.
*   **Compatibilidade de Conexão**: Suporte transparente para conexões concorrentes de diferentes ferramentas (Airflow via `psycopg2`, dbt-postgres e scripts produtores em Python).

## Opções Consideradas

### Opção 1: Múltiplas Instâncias Containerizadas de PostgreSQL
Provisionamento de dois containers independentes no Docker Compose (`marketplace-postgres-airflow` na porta 5432 e `marketplace-postgres-app` na porta 5433).
*   **Prós**:
    *   Isolamento físico total dos processos e arquivos de banco de dados.
*   **Contras**:
    *   Duplicação do consumo de memória RAM e recursos da CPU no host.
    *   Maior complexidade na gestão de volumes e portas expostas.

### Opção 2: Único Banco de Dados com Separação por Schemas
Utilização de um único banco de dados (ex: `marketplace`), diferenciando metadados do Airflow e dados da aplicação apenas por schemas PostgreSQL.
*   **Prós**:
    *   Configuração inicial simplificada.
*   **Contras**:
    *   Risco de contaminação de metadados durante resets de banco de dados (`db reset`) ou testes do dbt.
    *   Dificuldade de controle de privilégios e permissões de usuários.
    *   Incompatibilidade com o comando padronizado `airflow db init` que assume banco dedicado.

### Opção 3: Instância Única Dual-Role com Segregação por Bancos e Schemas via Script de Entrada (Recomendada)
Utilização de um único container de PostgreSQL 16 Alpine (`marketplace-postgres`), segregando dois bancos de dados independentes (`airflow` e `marketplace`), com o script declarativo [init.sql](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/postgres/init.sql) montado no diretório especial `/docker-entrypoint-initdb.d/` do container.
*   **Prós**:
    *   Eficiência máxima de hardware rodando apenas um container de banco.
    *   Isolamento lógico absoluto: o banco `airflow` armazena exclusivamente os metadados de orquestração, enquanto o banco `marketplace` abriga os schemas `transactional`, `staging`, `intermediate` e `marts`.
    *   Provisionamento 100% automatizado, determinístico e idempotente no primeiro arranque do container.
*   **Contras**:
    *   Falha pontual no container do PostgreSQL impacta simultaneamente a orquestração e a aplicação durante os testes locais.

## Decisão

Escolhemos a **Opção 3: Instância Única Dual-Role com Segregação por Bancos e Schemas via Script de Entrada**.

### Justificativa (Rationale)

A segregação em nível de banco de dados dentro de um mesmo container PostgreSQL 16 Alpine oferece isolamento perfeito com menor pegada de memória. A inclusão do script [init.sql](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/postgres/init.sql) em `/docker-entrypoint-initdb.d/init.sql` assegura que:
1. O banco `airflow` seja criado e receba privilégios administrativos para o usuário de orquestração.
2. O banco `marketplace` conte nativamente com os schemas de domínio de engenharia de dados: `transactional` (OLTP cru), `staging` (limpeza dbt), `intermediate` (regras de negócio dbt) e `marts` (modelagem dimensional de Gold).

## Consequências

### Positivas
*   Separação transparente de Connection Strings:
    *   Airflow: `postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/airflow`
    *   Marketplace App / dbt: `postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/marketplace`
*   Inicialização automática de todos os schemas na subida do container sem necessidade de execução manual de migrações DDL.
*   Persistência unificada e eficiente através do volume gerenciado `postgres_data`.

### Negativas
*   Caso o volume `postgres_data` seja destruído com `docker compose down -v`, ambos os bancos são limpos simultaneamente.

### Riscos e Mitigações
*   **Risco**: Race condition na subida do Airflow tentando acessar o banco `airflow` antes do script `/docker-entrypoint-initdb.d/init.sql` concluir a execução.
    *   *Mitigação*: Configuração de `healthcheck` via `pg_isready` no serviço `postgres` do [docker-compose.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml) e inclusão da diretiva `depends_on: postgres: condition: service_healthy` nos containers de inicialização e execução do Airflow.

## Referências
*   [init.sql](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/postgres/init.sql)
*   [docker-compose.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml)
