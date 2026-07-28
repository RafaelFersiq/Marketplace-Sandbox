# ADR-0005: Padrões de Qualidade de Código, Linting e Pipeline de CI/CD

## Status

Accepted

## Contexto e Declaração do Problema

O desenvolvimento do Marketplace Sandbox conta com a atuação contínua de múltiplos subagentes de IA especializados e desenvolvedores criando e mantendo componentes de código em Python 3.12+ (produtores de streaming Kafka, consumidores PySpark, DAGs Airflow, utilities e scripts de teste).

Sem regras estritas e automatizadas de governança de código, o projeto correria riscos de inconsistências estilísticas, dívida técnica rápida, ausência de type hints, padrões obsoletos de código e introdução de regressões ou bugs na branch principal (`main`). Precisamos estabelecer padrões claros de linting, formatação e suíte de testes unitários integrados a uma esteira automatizada de CI/CD (Integração Contínua).

## Direcionadores da Decisão (Decision Drivers)

*   **Velocidade e Eficiência de Ferramental**: Adotar ferramentas de linting e formatação com tempo de resposta quase instantâneo para não desacelerar o ciclo de desenvolvimento dos agentes.
*   **Centralização de Configuração**: Unificar todas as configurações de ferramentas Python em um único arquivo padrão de mercado ([pyproject.toml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/pyproject.toml)).
*   **Qualidade e Manutenibilidade do Código**: Garantir adesão estrita ao PEP 8, ordenação automática de imports, detecção de bugs potenciais (`flake8-bugbear`), código moderno (`pyupgrade`) e tipagem estática.
*   **Automação na Integração (CI/CD)**: Bloquear automaticamente merges ou pushes que violem padrões de qualidade ou quebrem testes unitários antes de atingirem a branch `main`.

## Opções Consideradas

### Opção 1: Conjunto Tradicional de Ferramentas (Flake8 + Black + isort + Pyupgrade)
Uso da combinação tradicional de bibliotecas Python independentes para manutenção de estilo e análise estática.
*   **Prós**:
    *   Ferramentas amplamente difundidas e maduras na comunidade Python.
*   **Contras**:
    *   Execução lenta em projetos com múltiplos arquivos.
    *   Incompatibilidades frequentes entre as regras do Black e do Flake8.
    *   Necessidade de múltiplos arquivos de configuração (`.flake8`, `setup.cfg`, `pyproject.toml`).

### Opção 2: Validação Manual de Código (Sem Pipeline CI/CD Automatizada)
Confiar exclusivamente em verificações manuais executadas pelos desenvolvedores ou subagentes antes dos commits.
*   **Prós**:
    *   Ausência de tempo de configuração de workflows no GitHub Actions.
*   **Contras**:
    *   Alto risco de regressões e quebras silenciosas na branch `main`.
    *   Perda de padronização nos commits gerados por agentes autônomos.

### Opção 3: Python 3.12+ com Ruff, Pytest e Pipeline Automatizado via GitHub Actions (Recomendada)
Centralização da governança de código no Python 3.12+, utilizando **Ruff** como linter e formatador unificado configurado em [pyproject.toml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/pyproject.toml), **Pytest** para execução de testes unitários e de integração, e **GitHub Actions** ([ci.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.github/workflows/ci.yml)) para automação das validações em eventos de `push` e `pull_request` na branch `main`.
*   **Prós**:
    *   Desempenho extremo: Ruff é escrito em Rust e roda até 100x mais rápido que linters tradicionais.
    *   Substitui Flake8, Black, isort, pyupgrade, flake8-bugbear e pep8-naming em um único pacote e uma única configuração no `pyproject.toml`.
    *   Validação estrita e determinística no GitHub Actions que impede a integração de código fora dos padrões.
*   **Contras**:
    *   Exige alinhamento rigoroso dos agentes com o conjunto de regras selecionado no `pyproject.toml`.

## Decisão

Escolhemos a **Opção 3: Python 3.12+ com Ruff, Pytest e Pipeline Automatizado via GitHub Actions**.

### Justificativa (Rationale)

O Ruff foi selecionado por sua velocidade incomparável e por centralizar o ecossistema de linting/formatação no [pyproject.toml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/pyproject.toml). O Pytest garante a execução de testes automatizados localizados na pasta `tests/`. O workflow do GitHub Actions em [.github/workflows/ci.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.github/workflows/ci.yml) impõe a execução obrigatória das etapas `ruff check .`, `ruff format --check .` e `pytest` a cada push ou PR, assegurando que a branch `main` esteja sempre verde e estável.

## Consequências

### Positivas
*   Garantia de código limpo, formatado e padronizado em todo o repositório.
*   Feedback ultrarrápido durante o desenvolvimento local e execuções no CI.
*   Centralização da configuração em `pyproject.toml`, simplificando a manutenção.
*   Proteção automatizada da branch principal alinhada ao GitHub Flow (definido na [ADR-0001](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/0001-git-branching-strategy.md)).

### Negativas
*   Código legado ou novos commits de agentes que violem regras do Ruff exigem correção imediata antes da aprovação no CI.

### Riscos e Mitigações
*   **Risco**: Incompatibilidade entre as versões do Python no ambiente local e no runner do GitHub Actions.
    *   *Mitigação*: Fixar explicitamente o Python 3.12 no `pyproject.toml` (`target-version = "py312"`) e no arquivo `.github/workflows/ci.yml` (`python-version: "3.12"`).

## Referências
*   [pyproject.toml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/pyproject.toml)
*   [.github/workflows/ci.yml](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/.github/workflows/ci.yml)
*   [ADR-0001: Estratégia de Branching do Git](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/adr/0001-git-branching-strategy.md)
*   [Ruff Official Documentation](https://docs.astral.sh/ruff/)
