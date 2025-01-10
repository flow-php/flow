# Static Analysis Baseline

Proposed by: @norberttech
Date: 2025-01-07

## Context
---

After removal of Psalm and making PHPStan the main static analysis tool, 
we looked again into hardening the static analysis configuration.

We had three globally ignored errors in phpstan configuration

- identifier: argument.type
- identifier: missingType.iterableValue
- identifier: missingType.generics

All of the above were significantly reducing the value of static analysis and 
code quality.

One of the proposed approaches was to use a baseline file to suppress those errors
in the existing codebase and gradually remove them.

There are few problems with this approach, but the most significant one is that once the baseline is introduced, 
it needs to be maintained.  
Whenever the code is changed, the baseline needs to be updated, which is an opportunity to also suppress new errors.  
This means that maintainers would not only need to go through the code changes
but also through the baseline file, which is not the best use of their very limited and valuable time anyway.

## Decision
---

We **must not** use baseline for static analysis. 
Instead, errors can be suppressed by annotations in the codebase or globally
in the static analysis tool configuration.

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
