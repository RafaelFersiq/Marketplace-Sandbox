# Diário de Aprendizado & Guia de Estudos Educacional

Bem-vindo ao **Diário de Aprendizado da Plataforma de Dados do Marketplace**. Este repositório foi projetado não apenas como uma plataforma de Engenharia de Dados em nível de produção, mas também como um guia de estudo interativo que documenta cada decisão arquitetural, trade-off de engenharia, aprofundamento conceitual e lição aprendida durante o desenvolvimento.

---

## 📚 Padrão de Nomenclatura & Estrutura de Diretórios

Todos os documentos educativos de sessão são organizados rigorosamente dentro de diretórios correspondentes às etapas do **Roadmap do Projeto** (`docs/context/roadmap.md`), seguindo o padrão de nomenclatura:

```text
docs/learning/
├── README.md                                                       <-- Índice Principal
└── step-NN-<nome-da-etapa-no-roadmap>/                             <-- Pasta do Step do Roadmap
    └── YYYY-MM-DD-session-NN-<tópico-da-sessão-em-ingles>.md      <-- Logs por Sessão Executada
```

### Exemplo Prático de Estrutura:
```text
docs/learning/
├── README.md
├── step-01-initial-project-structuring/
│   ├── 2026-07-23-session-01-workspace-setup-and-git-governance.md
│   ├── 2026-07-25-session-02-postgres-database-initialization.md
│   ├── 2026-07-26-session-03-containerized-infrastructure-docker-compose.md
│   └── 2026-07-26-session-04-local-service-verification-and-cicd-tooling.md
├── step-02-business-domain-modeling/
│   ├── 2026-07-26-session-05-business-events-specification.md
│   ├── 2026-07-28-session-06-avro-schemas-data-contracts-and-schema-registry.md
│   └── 2026-07-28-session-07-avro-logical-types-cache-and-backward-compatibility.md
└── step-03-building-event-producers/
    └── 2026-07-29-session-08-base-producer-framework-and-synthetic-generation.md
```

---

## 🗺️ Logs de Sessão do Roadmap

### Step 1 — Estruturação Inicial do Projeto
* [Sessão 01: Configuração do Workspace, Estratégia de Branching e Variáveis de Ambiente](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/learning/step-01-initial-project-structuring/2026-07-23-session-01-workspace-setup-and-git-governance.md) *(23/07/2026)*
* [Sessão 02: Inicialização do PostgreSQL, Separação de Metadados e Schemas da Arquitetura Medalhão](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/learning/step-01-initial-project-structuring/2026-07-25-session-02-postgres-database-initialization.md) *(25/07/2026)*
* [Sessão 03: Infraestrutura Containerizada da Plataforma via Docker Compose](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/learning/step-01-initial-project-structuring/2026-07-26-session-03-containerized-infrastructure-docker-compose.md) *(26/07/2026)*
* [Sessão 04: Verificação dos Serviços Locais, Esteira de CI/CD e Ferramental de Qualidade](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/learning/step-01-initial-project-structuring/2026-07-26-session-04-local-service-verification-and-cicd-tooling.md) *(26/07/2026)*

### Step 2 — Modelagem de Domínio de Negócio
* [Sessão 05: Especificação dos Eventos de Negócio do Marketplace](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/learning/step-02-business-domain-modeling/2026-07-26-session-05-business-events-specification.md) *(26/07/2026)*
* [Sessão 06: Esquemas Avro, Contratos de Dados e Confluent Schema Registry](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/learning/step-02-business-domain-modeling/2026-07-28-session-06-avro-schemas-data-contracts-and-schema-registry.md) *(28/07/2026)*
* [Sessão 07: Refinamento de Schemas Avro com Tipos Lógicos, Cache LRU e Compatibilidade Backward](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/learning/step-02-business-domain-modeling/2026-07-28-session-07-avro-logical-types-cache-and-backward-compatibility.md) *(28/07/2026)*

### Step 3 — Construção de Produtores de Eventos
* [Sessão 08: Framework Base de Produtores Kafka e Motor de Geração Sintética de Eventos](file:///home/rafael-siqueira/estudos/projetos/Marketplace-Sandbox/docs/learning/step-03-building-event-producers/2026-07-29-session-08-base-producer-framework-and-synthetic-generation.md) *(29/07/2026)*

---

## 💡 Estrutura dos Documentos (10 Seções Obrigatórias)

Cada log de sessão gerado pelo `educational-agent` contém 10 seções detalhadas:
1. **Resumo Executivo & Visão Geral**
2. **Arquitetura & Decisões de Design**
3. **Conceitos Fundamentais**
4. **Implementação Passo a Passo**
5. **Análise de Código e Trechos Chave**
6. **Desafios Enfrentados e Soluções**
7. **Trade-offs de Engenharia & Alternativas Consideradas**
8. **Boas Práticas e Padrões Recomendados**
9. **Perspectiva da Indústria e Casos Reais**
10. **Exercícios Práticos e Próximos Passos**
