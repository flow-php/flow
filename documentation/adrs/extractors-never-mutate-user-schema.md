# Extractors Never Mutate a User-Provided Schema

[TOC]

Proposed by: @norberttech
Date: 2026-07-27

## Context
---

`Schema` is intentionally mutable — `add()`, `remove()`, `rename()` and similar methods mutate `$this` and return
`$this`. File extractors extend the schema they were given so the hydrator can cast auto-added columns
(`_input_file_uri`, partition columns, sheet metadata): `Hydrator::cast()` iterates schema definitions and drops
undeclared row keys, so a column that should materialize in rows must be present in the schema.

An extractor that stores the caller's `Schema` instance therefore writes every internal extension into the
caller's object. This produced three observable defects (#2536 regression):

1. **Caller schema pollution** — a `Schema` the user holds for other purposes (e.g. passed later to a loader)
   gains non-nullable columns it never declared.
2. **Extractor-lifetime pollution** — columns added during one `extract()` run persist on the extractor's schema
   into subsequent runs.
3. **Cross-stream pollution** — partition columns of one stream leak into the schema used for the next stream,
   and `Hydrator::cast(fillMissing: true)` injects `null` into a non-nullable definition.

## Decision
---

**Extractors never mutate a user-provided `Schema`. Isolation is achieved by cloning, not by changing Schema's
mutable contract.**

Two rules apply to every extractor that accepts a `Schema`:

- **Clone at reception**: `withSchema()` (or a constructor argument) stores `clone $schema`, never the instance
  itself. This applies to *all* schema-receiving extractors, including those that do not extend the schema today —
  the rule is structural, so future mutations can never alias the caller.
- **Clone before extending**: an extractor that adds columns clones its own copy once per `extract()` run, and
  once more per stream before adding partition columns, so runs and streams never share a `Schema` instance.

A shallow `clone` is sufficient: `Schema`'s only state is `array<string, Definition>`, `add()` rebuilds the array,
and extractors never mutate existing `Definition` objects.

DSL `from_*()` functions stay pure delegation — the clone lives in the extractor, never in the DSL.

## Pros & Cons
---

**Advantages:**

- **No API change**: Schema's mutable contract and every call site stay untouched.
- **Structural safety**: aliasing is impossible regardless of what an extractor does to its copy later.
- **Cheap**: one shallow clone per reception, per run and per stream — negligible against I/O-bound extraction.

**Disadvantages:**

- The rule is convention, not compiler-enforced — a new extractor can forget to clone; regression tests asserting
  the caller's schema is untouched guard each extractor.
- Reception clones in non-extending extractors are unobservable dead weight until a mutation is introduced.

## Alternatives Considered
---

### 1. Make `Schema` immutable (`add()` returns a new instance)

**Rejected because:** it changes a core contract used across the entire codebase for a problem local to
extractors, and the mutable contract is a deliberate performance choice.

### 2. Materialize auto-added columns post-hydration via `Row::add()` (Floe style)

**Rejected because:** the extended schema *is* the hydrator's instruction set — `Hydrator::cast()` drops
undeclared row keys, and the `findDefinition()` guard lets a user-declared partition column keep its
user-defined type. Post-hydration adds would bypass both.

## Links and References
---

- [PR #2536](https://github.com/flow-php/flow/pull/2536) - encoder/hydrator row contract that introduced the
  schema extension in extractors
