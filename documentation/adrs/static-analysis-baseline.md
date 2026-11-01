# Static Analysis Baseline

[TOC]

Proposed by: @norberttech
Date: 2025-01-07

## Context
---

Mago is the main static analysis tool for this project. It is both the analyzer and the linter. It replaced
PHPStan, which itself replaced Psalm.

When we made the static analysis configuration stricter, we looked at the errors that were globally ignored to keep
the analysis green. Examples were full categories of missing or imprecise types. The global ignore made the static
analysis and the code quality much less useful.

One proposed approach was a baseline file. It suppresses those errors in the existing codebase, and we remove them
step by step. Mago supports baselines for both
`mago analyze` and `mago lint` through `--baseline`, `--generate-baseline` and
`--verify-baseline`.

This approach has a few problems. The most important problem: after the baseline is added, it needs maintenance.
Each code change needs an update of the baseline, and each update can also suppress new errors.
Maintainers then have to review the code changes and also the baseline file. This is not a good use of their very
limited time.

## Decision
---

We must not use a baseline for static analysis.
Instead, errors can be suppressed with annotations in the codebase,
`// @mago-expect <category>:<code>` and `// @mago-ignore <category>:<code>`, 
or globally in `mago.toml`.

Error suppression is an edge case. Use it rarely.
Core contributors must review and approve all suppression annotations.

## Pros & Cons
---

- The codebase is cleaner and easier to maintain.
- More predictable and stricter types.
- Less maintenance overhead related to managing the baseline.

## Links and References
---

- #1329
