# Schema Immutability

[TOC]

Proposed by: @norberttech
Date: 2026-07-27

## Context
---

`Schema` was mutable. `add()`, `remove()`, `rename()`, `merge()` and all other mutators rewrote
`$this->definitions` and returned `$this`. `Definition::addMetadata()` and `Definition::setMetadata()` did the same
with `$this->metadata`.

File extractors extend the schema that they get, so that the hydrator can cast auto-added columns
(`_input_file_uri`, partition columns, sheet metadata). `Hydrator::cast()` iterates the schema definitions and drops
undeclared row keys. A column that must appear in rows must therefore be in the schema. An extractor that stored the
`Schema` of the caller therefore wrote each internal extension into the object of the caller. This caused three
visible defects (#2536 regression):

1. Caller schema pollution: a `Schema` that the user holds for other purposes gets non-nullable columns that it never
   declared.
2. Extractor-lifetime pollution: columns added during one `extract()` run stay in the next runs.
3. Cross-stream pollution: partition columns of one stream go into the next stream, and
   `Hydrator::cast(fillMissing: true)` injects `null` into a non-nullable definition.

The first fix cloned. `withSchema()` stored `clone $schema`, and extractors cloned again for each run and each
stream. That fix was incomplete. `clone` is shallow, and the only state of `Schema` is `array<string, Definition>`. A
cloned `Schema` therefore shares its `Definition` instances. `Schema::addMetadata()` / `setMetadata()` changed a
shared `Definition` in place, so each metadata write went through to all copies, also the copy of the caller.

The same mutable contract left hidden aliasing traps in other places:

- `Rows::schema()` started its merge loop with the memoized `Schema` of row 0 and corrupted it.
- `FloeStreamWriter` kept a `Schema` of the caller for the lifetime of a write session.
- The fast paths of `merge()` returned `$this` or the argument.

## Decision
---

`Schema` and its full state chain are immutable. Each mutator returns a new instance. Nothing is ever written in
place.

- `Schema`: a `final readonly class`. All 19 mutators return `new self(...)`. `setDefinitions()` is the validation
  helper of the constructor, and only the constructor calls it.
- `Definition` (19 implementations): each one is a `final readonly class`. `addMetadata()` and `setMetadata()` return
  a `new self(...)` of their own class. `makeNullable()` and `rename()` already used this idiom.
- `Metadata`: already a `final readonly class`.

Immutability is declared at the class level, not for each property. A `readonly class` cannot get a writable
property later. The guarantee therefore survives future edits. It does not depend on the person who adds property
number 20 and remembers the rule. The compiler enforces it, so it is not a convention. Extractors and the DSL hold the
`Schema` instances of the caller directly. There is nothing to clone, because there is nothing to mutate. Shared
instances are safe by construction. Examples are the fast paths of `merge()`, a kept base `Definition` in the
hydrator, and the memoized schema of `Rows`.

DSL `from_*()` functions stay pure delegation.

### Out of scope

`UnresolvedReference` stays mutable. `as()`, `asc()` and `desc()` write `$alias` / `$sort` on `$this`. The full
expression DSL shares it, so its immutability is a separate project. This decision does not try it.

### Breaking change

A call to a mutator that discards the result is now a silent no-op. There is no `#[\NoDiscard]`.
[upgrading.md](/documentation/upgrading.md) tells users about the change.

```php
$schema->add(str_schema('x'));          // before: mutates $schema. now: no-op.
$schema = $schema->add(str_schema('x')); // correct
```

## Pros & Cons
---

Advantages:

- The bug class is gone, not patched. Aliasing is impossible, because there is no writable state to alias.
- Compiler-enforced: a `readonly` violation is a fatal error, not a convention that a new extractor can forget.
- Sharing is free: no defensive clones in extractors, `PhpRowHydrator`, or the native hydrator.
- Fixes the `Rows::schema()` and `FloeStreamWriter` aliasing traps without touching either.

Disadvantages:

- Breaking change for downstream code that discards the return value of a mutator, and it breaks silently.
- A long mutator chain allocates one `Schema` for each link. Schemas are small and built once for each pipeline, not
  for each row, so this does not show in profiles.

## Alternatives Considered
---

### 1. Clone at reception, clone before extending

Each `withSchema()` stores `clone $schema`. Extractors clone again for each run and each stream.

Rejected because: the clone is shallow, so `Definition` instances stay shared, and metadata mutations still go
through to all copies. It is also a convention and not a rule that the compiler enforces. A new extractor can forget
to clone. It also leaves invisible dead clones in extractors that never extend the schema.

### 2. Deep `Schema::__clone()` + `Definition::__clone()`

Give `Schema` and each `Definition` a `__clone()` that copies the definition array and its objects.

Rejected because: it patches the symptom and keeps the mutable contract. Each future aliasing trap
(`Rows::schema()`, `FloeStreamWriter`, the fast paths of `merge()`) must still be found and cloned around by hand. It
also makes each clone more expensive, and developers must still remember to clone.

### 3. Materialize auto-added columns post-hydration via `Row::add()` (Floe style)

Rejected because: the extended schema is the instruction set of the hydrator. `Hydrator::cast()` drops undeclared
row keys. The `findDefinition()` guard lets a partition column that the user declared keep its user-defined type.
Adds after hydration go around both.

## Links and References
---

- [PR #2536](https://github.com/flow-php/flow/pull/2536): the encoder/hydrator row contract that added the
  schema extension in extractors
