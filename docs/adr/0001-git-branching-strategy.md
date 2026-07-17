# ADR-0001: Estratégia de Branching do Git

## Status

Accepted

## Contexto e Declaração do Problema

Este projeto conta com o desenvolvimento coordenado por múltiplos agentes especialistas executando tarefas em paralelo através de Git Worktrees (modo de workspace `share` do Antigravity). Precisamos de uma estratégia de versionamento (branching) simples, que minimize conflitos de merge, evite poluição na branch principal e seja fácil de ser seguida por agentes autônomos.

## Direcionadores da Decisão (Decision Drivers)

*   **Simplicidade operacional**: O fluxo deve ter baixa complexidade para automação dos agentes.
*   **Isolamento**: Cada agente deve trabalhar de forma isolada em sua própria ramificação sem impactar o trabalho ativo de outros.
*   **Proteção da branch estável**: A branch principal deve refletir sempre um estado funcional e testado da plataforma.

## Opções Consideradas

### Opção 1: Git Flow
Modelo tradicional baseado em branches de `master`, `develop`, `feature/*`, `release/*` e `hotfix/*`.
*   **Prós**: Altamente estruturado, adequado para ciclos de lançamento de software tradicionais.
*   **Contras**: Excessivamente complexo para um sandbox de engenharia de dados. Exige múltiplos merges (ex: feature para develop, develop para release, release para master e develop), aumentando o risco de conflitos complexos para agentes automatizados.

### Opção 2: GitHub Flow (Recomendado)
Modelo baseado em uma única branch estável (`main`) e branches de feature de curta duração (`feature/*` ou `infra/*`), com merges diretos via Pull Requests.
*   **Prós**: Extremamente simples, linear e ágil. Combina perfeitamente com subagentes transientes criados via Git Worktree. O fluxo de desenvolvimento é: criar branch $\rightarrow$ commitar $\rightarrow$ abrir PR $\rightarrow$ merge na `main`.
*   **Contras**: Exige testes automatizados rigorosos na branch `main` e em PRs para evitar quebras silenciosas.

## Decisão

Escolhemos a **Opção 2: GitHub Flow** para guiar o versionamento do projeto.

### Justificativa (Rationale)
O GitHub Flow oferece a menor sobrecarga de gerenciamento para os agentes, permitindo que cada subagente trabalhe de forma totalmente isolada em uma branch dedicada criada a partir da `main` (ex: `feature/data-producers`). Quando a tarefa é concluída e validada, a branch é integrada diretamente à `main`.

## Consequências

### Positivas
*   Linearidade de desenvolvimento.
*   Isolamento completo: subagentes paralelos usam `git worktree` apontando para branches de feature diferentes (ex: `feature/kafka-setup` e `feature/spark-streaming`) sem interrupção mútua.
*   Processo simplificado de Integração Contínua (CI).

### Negativas
*   Exige disciplina dos agentes para manter commits atômicos e descritivos.
*   Qualquer quebra na branch `main` afeta imediatamente os novos worktrees criados pelos agentes subsequentes.

### Riscos e Mitigações
*   **Risco**: Quebra acidental da `main` por merge incorreto.
    *   *Mitigação*: Configuração de esteira de CI com testes de integração e linters obrigatórios em todos os PRs (etapa final do Step 1).

## Referências
*   [GitHub Flow Documentation](https://docs.github.com/en/get-started/using-git/github-flow)
