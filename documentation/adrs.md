# Architecture Decision Record

# What are ADRs?
Architecture Decision Records (ADRs) are structured documents that capture significant architectural decisions made during the development of this project. 

Each ADR explains:
- **The context**: Why the decision was necessary.  
- **The decision**: What was decided and why.  
- **The consequences**: The impact of the decision, both positive and negative.  

ADRs act as a single source of truth for important design and architectural choices, ensuring that everyone involved in the project has a shared understanding of why things are the way they are.

## How to Use ADRs in This Project
**Follow Existing ADRs**: ADRs are mandatory to follow unless explicitly overridden by a new ADR. This ensures consistency in decision-making across the project.

> Whenever ADR is merged into the main branch, it is considered accepted and must be followed by all contributors.

**Propose New ADRs for Significant Decisions**:

- If you encounter a situation that requires a significant architectural or design change, you must create a new ADR.
- Use the provided [ADR Template](/documentation/adrs/template.md) to create your proposal.
- Submit the ADR via a pull request and engage in a discussion with maintainers and contributors.
- **Document Decisions Transparently**: All significant decisions must be documented. This includes not only what was decided but also the alternatives that were considered and why they were rejected.

## Index of ADRs

### [Accepted AD](https://github.com/flow-php/flow/pulls?q=is%3Apr+is%3Aclosed+is%3Amerged+label%3AAD+)
- [2025-01-07: Static Analysis Baseline](/documentation/adrs/static-analysis-baseline.md)
- [2025-01-09: Extension Points](/documentation/adrs/extension-points.md)

### [Proposed AD](https://github.com/flow-php/flow/pulls?q=is%3Apr+is%3Aopen+label%3AAD+)

### [Rejected AD](https://github.com/flow-php/flow/pulls?q=is%3Apr+is%3Aclosed+is%3Aunmerged+label%3AAD+)