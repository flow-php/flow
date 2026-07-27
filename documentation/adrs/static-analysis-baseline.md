# Static Analysis Baseline

[TOC]

Proposed by: @norberttech
Date: 2025-01-07

## Context
---

Mago is the main static analysis tool for this project, providing both the analyzer 
and the linter. It replaced PHPStan, which itself replaced Psalm.

While hardening the static analysis configuration, we looked at the errors that were 
globally ignored to keep the analysis green, for example whole categories of missing 
or imprecise types. Ignoring them globally was significantly reducing the value of 
static analysis and code quality.

One of the proposed approaches was to use a baseline file to suppress those errors
in the existing codebase and gradually remove them. Mago supports baselines for both
`mago analyze` and `mago lint` through `--baseline`, `--generate-baseline` and
`--verify-baseline`.

There are few problems with this approach, but the most significant one is that once the baseline is introduced, 
it needs to be maintained.  
Whenever the code is changed, the baseline needs to be updated, which is an opportunity to also suppress new errors.  
This means that maintainers would not only need to go through the code changes
but also through the baseline file, which is not the best use of their very limited and valuable time anyway.

## Decision
---

We **must not** use baseline for static analysis. 
Instead, errors can be suppressed by annotations in the codebase, 
`// @mago-expect <category>:<code>` and `// @mago-ignore <category>:<code>`, 
or globally in `mago.toml`.

Error suppression should be considered an edge case and should be used sparingly.
Core contributors should review and approve all suppression annotations.

## Pros & Cons
---

- The codebase will be cleaner and more maintainable.
- More predictable and stricter types.
- Less maintenance overhead related to managing the baseline.

## Links and References
---

- #1329
