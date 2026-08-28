# Schema Immutability

[TOC]

Proposed by: @norberttech
Date: 2026-07-27

## Context
---

`Schema` was mutable - `add()`, `remove()`, `rename()`, `merge()` and every other mutator rewrote
`$this->definitions` and returned `$this`. `Definition::addMetadata()` and `Definition::setMetadata()` did the same
with `$this->metadata`.

File extractors extend the schema they were given so the hydrator can cast auto-added columns
(`_input_file_uri`, partition columns, sheet metadata): `Hydrator::cast()` iterates schema definitions and drops
undeclared row keys, so a column that should materialize in rows must be present in the schema. An extractor
storing the caller's `Schema` therefore wrote every internal extension into the caller's object, producing three
observable defects (#2536 regression):

1. **Caller schema pollution** - a `Schema` the user holds for other purposes gains non-nullable columns it never
   declared.
2. **Extractor-lifetime pollution** - columns added during one `extract()` run persist into subsequent runs.
3. **Cross-stream pollution** - partition columns of one stream leak into the next, and
   `Hydrator::cast(fillMissing: true)` injects `null` into a non-nullable definition.

The first fix cloned: `withSchema()` stored `clone $schema`, and extractors cloned again per run and per stream.
That fix was incomplete. `clone` is shallow and `Schema`'s only state is `array<string, Definition>`, so a cloned
`Schema` **shares its `Definition` instances**. `Schema::addMetadata()` / `setMetadata()` reached into a shared
`Definition` and mutated it in place, so metadata writes aliased through every copy - including the caller's.

The same mutable contract left latent aliasing traps elsewhere: `Rows::schema()` seeded its merge loop with row 0's
*memoized* `Schema` and corrupted it, `FloeStreamWriter` retained a caller-owned `Schema` for the lifetime of a
write session, and `merge()`'s fast paths returned `$this` or the argument.

## Decision
---

**`Schema` and its whole state chain are immutable. Every mutator returns a new instance; nothing is ever written
in place.**

- `Schema` - a `final readonly class`. All 19 mutators return `new self(...)`; `setDefinitions()` is the
  constructor's validation helper and is called from the constructor only.
- `Definition` (19 implementations) - each a `final readonly class`. `addMetadata()` and `setMetadata()` return a
  per-class `new self(...)`, matching the idiom `makeNullable()` and `rename()` already used.
- `Metadata` - already a `final readonly class`.

Immutability is declared at the class level, not per property: a `readonly class` cannot gain a writable property
later, so the guarantee survives future edits instead of depending on whoever adds property number 20 remembering
the rule. It is compiler-enforced, not convention. Extractors and the DSL hold caller-provided `Schema` instances
directly: there is nothing to clone because there is nothing to mutate. Sharing an instance - `merge()`'s fast
paths, a retained base `Definition` in the hydrator, `Rows`' memoized schema - is safe by construction.

DSL `from_*()` functions stay pure delegation.

### Out of scope

`UnresolvedReference` remains mutable - `as()`, `asc()` and `desc()` write `$alias` / `$sort` on `$this`. It is shared
with the entire expression DSL, so making it immutable is a separate project and is not attempted here.

### Breaking change

Calling a mutator and discarding the result is now a **silent no-op**. There is no `#[\NoDiscard]`; the change is
communicated through [upgrading.md](/documentation/upgrading.md).

```php
$schema->add(str_schema('x'));          // before: mutates $schema. now: no-op.
$schema = $schema->add(str_schema('x')); // correct
```

## Pros & Cons
---

**Advantages:**

- **The bug class is gone**, not patched - aliasing is impossible because there is no writable state to alias.
- **Compiler-enforced**: a `readonly` violation is a fatal error, not a convention a new extractor can forget.
- **Sharing becomes free**: no defensive clones in extractors, `PhpRowHydrator`, or the native hydrator.
- Fixes the `Rows::schema()` and `FloeStreamWriter` aliasing traps without touching either.

**Disadvantages:**

- Breaking change for downstream code that discards a mutator's return value, and it breaks **silently**.
- A long mutator chain allocates one `Schema` per link. Schemas are small and built once per pipeline, not per
  row, so this does not show up in profiles.

## Alternatives Considered
---

### 1. Clone at reception, clone before extending

Every `withSchema()` stores `clone $schema`; extractors clone again per run and per stream.

**Rejected because:** the clone is shallow, so `Definition` instances stay shared and metadata mutations alias
through every copy anyway. It is also convention rather than a compiler-enforced rule - a new extractor can forget
to clone - and it leaves unobservable dead clones in extractors that never extend the schema.

### 2. Deep `Schema::__clone()` + `Definition::__clone()`

Give `Schema` and every `Definition` a `__clone()` that copies the definition array and its objects.

**Rejected because:** it patches the symptom while keeping the mutable contract, so every future aliasing trap
(`Rows::schema()`, `FloeStreamWriter`, `merge()`'s fast paths) still has to be found and cloned around by hand. It
also makes every clone more expensive without removing the need to remember to clone.

### 3. Materialize auto-added columns post-hydration via `Row::add()` (Floe style)

**Rejected because:** the extended schema *is* the hydrator's instruction set - `Hydrator::cast()` drops undeclared
row keys, and the `findDefinition()` guard lets a user-declared partition column keep its user-defined type.
Post-hydration adds would bypass both.

## Links and References
---

- [PR #2536](https://github.com/flow-php/flow/pull/2536) - encoder/hydrator row contract that introduced the
  schema extension in extractors
