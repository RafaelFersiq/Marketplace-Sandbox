# ADR-0003: Arquitetura de Infraestrutura Containerizada Local e Simulação Cloud

## Status

Accepted

## Contexto e Declaração do Problema

O projeto Marketplace Sandbox exige um ecossistema completo de engenharia de dados em tempo real e em lote, integrando barramento de eventos (Kafka), governança de schemas (Schema Registry), processamento distribuído (Spark), orquestração de workflows (Airflow), armazenamento de objetos (MinIO/S3), banco de dados relacional (PostgreSQL) e observabilidade (Prometheus e Grafana).

Precisamos de um ambiente de desenvolvimento local declarativo, reproduzível e isolado, capaz de emular serviços de nuvem (como Amazon S3) sem custos financeiros ou dependências externas durante o desenvolvimento. Além disso, o ambiente deve suportar a execução simultânea de múltiplos agentes e desenvolvedores operando em Git Worktrees isolados.

## Direcionadores da Decisão (Decision Drivers)

*   **Reprodutibilidade e Paridade de Ambientes**: Garantir a execução idêntica de toda a stack tecnológica em qualquer máquina host via containerização.
*   **Simulação Cloud Custos Zero**: Emulação local de Object Storage compatível com a API do AWS S3 (via MinIO) para armazenamento de dados nas camadas Bronze, Silver e Gold da arquitetura Medallion.
*   **Suporte a Git Worktrees (Isolamento de Portas)**: Permitir parametrização dinâmica de portas expostas no host através de variáveis de ambiente (`.env`) para evitar conflitos de bind (`port binding`) ao executar instâncias paralelas da stack.
*   **Gestão Unificada de Serviços**: Inicialização, gerenciamento e interdependência de containers coordenados declarativamente por um único arquivo de composição (`docker-compose.yml`).

## Opções Consideradas

### Opção 1: Instalação Nativa de Binários no Host (Local Binaries)
Instalação direta dos serviços (PostgreSQL, Kafka, Spark, Airflow, Prometheus) no sistema operacional local.
*   **Prós**:
    *   Desempenho nativo sem a sobrecarga de virtualização ou containers.
*   **Contras**:
    *   Dificuldade de padronização entre diferentes máquinas de desenvolvimento.
    *   Risco de contaminação e poluição do sistema operacional host.
    *   Impossibilidade prática de isolar portas para execuções simultâneas em múltiplos Git Worktrees.

### Opção 2: Provedores de Nuvem Reais (AWS Direct Infrastructure)
Provisionamento imediato de recursos em nuvem (Amazon MSK, EMR/Databricks, MWAA, RDS, S3) via Terraform.
*   **Prós**:
    *   Paridade exata com o ambiente final de produção.
*   **Contras**:
    *   Custos financeiros contínuos desde as etapas iniciais de sandbox e testes locais.
    *   Dependência de conectividade constante com a internet e credenciais da nuvem.
    *   Latência elevada nos ciclos iterativos de teste e desenvolvimento de pipelines.

### Opção 3: Stack Containerizada Local via Docker Compose com MinIO (Recomendada)
Orquestração declarativa em [docker-compose.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml) abrangendo 12 serviços containerizados (Kafka, Zookeeper, Schema Registry, Kafka UI, Spark Master/Worker, Airflow Init/Webserver/Scheduler, MinIO, PostgreSQL, Prometheus e Grafana).
*   **Prós**:
    *   Provisionamento determinístico com um único comando (`docker compose up -d`).
    *   Simulação de Data Lake com protocolo S3 100% compatível utilizando MinIO.
    *   Custo zero de infraestrutura e funcionamento offline.
    *   Mapeamento de portas expostas parametrizado via `.env`, garantindo isolamento completo entre Git Worktrees paralelos.
*   **Contras**:
    *   Exige recursos computacionais significativos no host (RAM e CPU) ao executar todos os serviços simultaneamente.

## Decisão

Escolhemos a **Opção 3: Stack Containerizada Local via Docker Compose com MinIO**.

### Justificativa (Rationale)

A solução via Docker Compose provê o equilíbrio perfeito entre autonomia local, velocidade de desenvolvimento e fidelidade arquitetural. O uso do MinIO permite que os pipelines em PySpark e DAGs do Airflow utilizem conectores padrão `s3a://` exatamente como fariam na AWS, sem incorrer em custos de nuvem. A parametrização de portas no arquivo `.env` resolve diretamente os requisitos de concorrência descritos na governança do projeto em [GEMINI.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/GEMINI.md).

## Consequências

### Positivas
*   Ciclo de vida de infraestrutura simples (`docker compose up -d` e `docker compose down`).
*   Healthchecks configurados para garantir a inicialização ordenada de serviços dependentes (ex: PostgreSQL $\rightarrow$ Airflow-init $\rightarrow$ Airflow Webserver/Scheduler).
*   Facilidade de integração com ferramentas de monitoramento e métricas (Prometheus e Grafana).

### Negativas
*   Consumo de memória RAM do host estimado em ~6GB-8GB ao manter a stack completa ativa.

### Riscos e Mitigações
*   **Risco**: Colisão de portas ao rodar múltiplos Git Worktrees na mesma máquina.
    *   *Mitigação*: Definir variáveis de porta no `.env` (ex: `${KAFKA_BROKER_PORT}`, `${POSTGRES_PORT}`) e documentar o procedimento de isolamento no [GEMINI.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/GEMINI.md).
*   **Risco**: Divergência comportamental entre MinIO local e Amazon S3 real.
    *   *Mitigação*: Utilizar exclusivamente bibliotecas e conectores oficiais da AWS (`boto3`, `hadoop-aws`) alterando apenas o parâmetro `endpoint_url`.

## Referências
*   [docker-compose.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/infrastructure/docker-compose.yml)
*   [GEMINI.md](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/GEMINI.md)
