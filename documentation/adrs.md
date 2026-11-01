# Architecture Decision Record

[TOC]

# What are ADRs?

Architecture Decision Records (ADRs) are structured documents. They record the important architectural decisions of
this project.

Each ADR explains:

- The context: why the decision was necessary.
- The decision: what was decided and why.
- The consequences: the positive and negative effects of the decision.

ADRs are the single source of truth for important design and architectural choices. Everyone in the project can
read in them why things are the way they are.

## How to Use ADRs in This Project

Follow existing ADRs: you must follow each ADR. Only a new ADR can explicitly override it. This keeps the
decisions in the project consistent.

> When an ADR is merged into the main branch, it is accepted. All contributors must then follow it.

Propose new ADRs for important decisions:

- If a situation needs an important architectural or design change, you must create a new ADR.
- Use the [ADR Template](/documentation/adrs/template.md) for your proposal.
- Send the ADR as a pull request, and discuss it with maintainers and contributors.
- Document decisions openly: all important decisions must be documented. The document says what was decided, and
  also which alternatives were considered and why they were rejected.

## Index of ADRs

### [Accepted AD](https://github.com/flow-php/flow/pulls?q=is%3Apr+is%3Aclosed+is%3Amerged+label%3AAD+)

- [2025-01-07: Static Analysis Baseline](/documentation/adrs/static-analysis-baseline.md)
- [2025-01-09: Extension Points](/documentation/adrs/extension-points.md)
- [2026-03-02: Variadic Arguments Pattern for Required Parameters](/documentation/adrs/variadic-arguments-pattern.md)
- [2026-07-27: Schema Immutability](/documentation/adrs/schema-immutability.md)

### [Proposed AD](https://github.com/flow-php/flow/pulls?q=is%3Apr+is%3Aopen+label%3AAD+)

### [Rejected AD](https://github.com/flow-php/flow/pulls?q=is%3Apr+is%3Aclosed+is%3Aunmerged+label%3AAD+)