# ADR-0006: Adoção do Modelo Argon 4 para Subagentes de Alto Raciocínio (High-Reasoning Tier)

## Status

Accepted

## Contexto e Declaração do Problema

Com o lançamento do modelo **Argon 4**, uma nova classe de modelos de fronteira com capacidades superiores de raciocínio lógico, planejamento abstrato e rigor arquitetural tornou-se disponível no ecossistema do projeto.

No entanto, em um ambiente multiagente como o Marketplace Sandbox, aplicar indiscriminadamente um modelo de fronteira (frontier model) a todos os subagentes introduz problemas operacionais significativos:
1. **Consumo e Esgotamento Precoce de Cota:** Modelos de ponta possuem limites de taxa mais restritos (Tokens por Minuto - TPM, Requisições por Minuto - RPM e limites diários de requisição).
2. **Latência de Resposta:** Modelos focados em raciocínio profundo possuem maior tempo de primeira resposta (*Time to First Token* - TTFT) e menor velocidade de geração por token quando comparados a modelos da classe Flash.
3. **Ineficiência de Custo/Recurso:** Tarefas de documentação sintética, geração de Markdown educacional e escrita de código rotineiro guiado por tipagem estrita operam perfeitamente em modelos balanceados e leves (`gemini-3.7-flash`).

É necessário definir formalmente quais subagentes devem utilizar o **Argon 4** e quais devem permanecer na camada balanceada/rápida.

## Direcionadores da Decisão (Decision Drivers)

*   **Risco Arquitetural e "Blast Radius":** Subagentes cujas decisões ou código afetam criticamente todo o projeto requerem máxima capacidade de raciocínio.
*   **Conservação de Quota:** Preservar a cota do modelo Argon 4 para atividades de alto impacto cognitivo, evitando 429 / `RESOURCE_EXHAUSTED` em loops de rotina.
*   **Velocidade e Iteratividade do Feedback:** Manter produtores de código, testes pytest e documentação rápidos para não degradar a experiência de desenvolvimento.
*   **Aderência às Diretrizes Multiagente (`GEMINI.md`):** Garantir conformidade com a segregação de responsabilidades e níveis de raciocínio.

## Opções Consideradas

### Opção 1: Migrar Todos os Subagentes para o Argon 4
*   **Prós**:
    *   Capacidade cognitiva máxima em todas as tarefas, incluindo documentação e testes.
*   **Contras**:
    *   Esgotamento acelerado de cotas diárias e rate limits em tarefas longas (como diários de 10 seções do `educational-agent`).
    *   Aumento expressivo na latência em tarefas triviais de síntese.

### Opção 2: Migração Seletiva com Base na Criticidade e Esforço de Raciocínio (Adotada)
*   **Prós**:
    *   Aloca o modelo **Argon 4** exclusivamente para subagentes com perfil de *High Effort*:
        *   `planning-agent`: Responsável pela decomposição atômica do roadmap em marcos técnicos no `current-step.md`. Erros aqui desestruturam todo o ciclo de desenvolvimento.
        *   `devops-infra-agent`: Responsável por Terraform IaC, isolamento de containers, segurança de redes, segredos e pipelines CI/CD no GitHub Actions. Erros aqui causam vulnerabilidades e bloqueios de infraestrutura.
    *   Mantém subagentes de implementação (`software-engineer-agent`, `data-engineer-agent`, `analytics-engineer-agent`, `observability-agent`) no tier balanceado (`gemini-3.7-flash` com esforço médio), onde o feedback rápido em loops TDD é prioritário.
    *   Mantém subagentes de documentação (`tech-writer-agent`, `educational-agent`) no tier leve (`gemini-3.7-flash` com baixo esforço), otimizando volume de tokens e geração em massa de Markdown conforme ADR-0002.
*   **Contras**:
    *   Exige governança na configuração de modelos em `.agents/agents/*/agent.md` e `GEMINI.md`.

## Decisão

Adotamos a **Opção 2**. O modelo **Argon 4** passa a ser o modelo oficial para o tier de alto raciocínio (*High Effort*), associado ao nível de modelo `pro` (aceito no frontmatter YAML `model: pro` e no parâmetro `Model: 'pro'` da ferramenta `invoke_subagent`).

Os subagentes configurados para o tier `pro` (Argon 4) são:
1. `planning-agent`
2. `devops-infra-agent`

Todos os demais subagentes continuam utilizando o tier `flash` (`gemini-3.7-flash`) configurado com esforço Médio ou Baixo.

### Justificativa (Rationale)

A segregação preserva a cota do Argon 4 para momentos estratégicos de alto risco, prevenindo interrupções por esgotamento de taxa e mantendo alta agilidade nas fases de codificação e geração de documentação educacional.

## Consequências

### Positivas
*   Maximização da qualidade do planejamento macro e da segurança da infraestrutura de código e CI/CD.
*   Proteção contra esgotamento precoce de cotas do modelo de fronteira.
*   Execução ágil de testes unitários e produção de documentação educacional.

### Negativas
*   Caso uma tarefa de codificação pontual necessite de raciocínio de fronteira além do `software-engineer-agent`, o agente central precisa explicitamente invocar com override de modelo.

### Riscos e Mitigações
*   **Risco**: Esgotamento temporário de cota do Argon 4 durante longas sessões de planejamento ou infraestrutura.
    *   *Mitigação*: O agente orquestrador possui estratégia de fallback para `inherit` ou `flash` caso ocorra `RESOURCE_EXHAUSTED`.

## Referências
*   [GEMINI.md](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/GEMINI.md)
*   [ADR-0002: Educational Documentation Subagent and Learning Journal Architecture](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/adr/0002-educational-agent-and-journal-structure.md)
*   [docs/adr/README.md](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/adr/README.md)
