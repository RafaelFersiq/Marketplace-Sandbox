# ADR-0002: Educational Documentation Subagent and Learning Journal Architecture

## Status

Accepted

## Context and Problem Statement

The Marketplace Sandbox project serves as an educational laboratory and portfolio for Data Engineering. Beyond building functional real-time infrastructure, capturing the engineering reasoning, trade-offs, architecture decisions, and core concepts behind every implementation is critical. 

Without an automated mechanism, session documentation tends to be ad-hoc, superficial, or lost over time. We need a dedicated subagent and a structured storage hierarchy that automatically converts completed roadmap implementations into a structured, long-term learning journal.

## Decision Drivers

* **Educational Value & Portfolio Quality**: Documenting *why* a solution was built, alternative designs, trade-offs, and enterprise production practices alongside code changes.
* **Workflow Automation**: Automatically triggering documentation generation only upon completing roadmap tasks, avoiding noise from discussions, planning, or refactoring.
* **Scalable Directory Organization**: A structured hierarchy in `docs/learning/` that grows chronologically and remains easy to browse as sessions accumulate.
* **Cost and Model Efficiency**: Operating documentation tasks on an inexpensive, token-efficient AI model (`gemini-3.6-flash`) with low reasoning overhead.

## Options Considered

### Option 1: Manual Documentation by Human/Central Agent
Central Agent or user manually updates documentation after every session.
* **Pros**: Direct control over content.
* **Cons**: Prone to inconsistency, easily forgotten after long coding sessions, cluttering Central Agent context window.

### Option 2: Combine Technical Writer (`tech-writer-agent`) for all documentation
Use `tech-writer-agent` to handle both system architecture diagrams/ADRs and session learning logs.
* **Pros**: Consolidates documentation tasks in a single agent.
* **Cons**: Overloads scope of `tech-writer-agent`. Mixes system-level documentation (C4 diagrams, ADRs) with session-based, step-by-step educational journals.

### Option 3: Dedicated Educational Subagent (`educational-agent`) with `docs/learning/` journal (Recommended)
Introduce a specialized `educational-agent` operating on `gemini-3.6-flash` that is triggered exclusively after sessions that complete roadmap tasks.
* **Pros**: Clear separation of concerns, optimized model selection (low cost, fast execution), standardized 10-section learning template, and chronological step-based folder structure.
* **Cons**: Requires explicit trigger management in the context workflow to prevent execution during non-roadmap sessions.

## Decision

We chose **Option 3: Dedicated Educational Subagent (`educational-agent`) with `docs/learning/` journal**.

### Rationale

A dedicated subagent ensures that educational documentation is treated as a first-class citizen with a uniform 10-section structure (Session Summary, What Was Built, Why Built This Way, Important Concepts, Alternative Approaches, Trade-offs, Industry Perspective, Common Mistakes, Further Reading, Key Takeaways). 

Using `gemini-3.6-flash` ensures maximum cost efficiency without sacrificing formatting precision or clarity.

## Consequences

### Positive
* Automatic generation of high-value study guides after every completed roadmap task.
* Clean separation between project architecture (ADRs, C4 diagrams in `docs/adr/`) and educational learning logs (`docs/learning/`).
* Token-efficient and cost-effective execution.

### Negative
* Requires Central Agent to explicitly check session outcome before invoking `educational-agent`.

### Risks and Mitigations
* **Risk**: Generating redundant documentation for incomplete or aborted sessions.
    * *Mitigation*: Strictly enforce the trigger rule in `GEMINI.md`—`educational-agent` is invoked *only* after `current-step.md` tasks are marked completed and session state is finalized.

## References
* [GEMINI.md Guidelines](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/GEMINI.md)
* [Roadmap Document](file:///home/rafael-siqueira/Estudos/Projetos/Marketplace-Sandbox/docs/context/roadmap.md)
