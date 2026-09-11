# Variadic Arguments Pattern for Required Parameters

[TOC]

Proposed by: @norberttech
Date: 2026-03-02

## Context
---

Several Flow PHP methods accept multiple arguments where at least one is required. Examples include:
- `DataFrame::partitionBy()`
- `Rows::partitionBy()`
- `Window::partitionBy()`
- Type comparison methods

A proposal was made to simplify the API signatures from:

```php ignore
public function partitionBy(string|Reference $entry, string|Reference ...$entries) : self
```

To:

```php ignore
public function partitionBy(string|Reference ...$entries) : self
```

The rationale for the proposal was to reduce array operations like `array_unshift()` used internally to merge the first required argument with the variadic rest.

## Decision
---

**Keep the explicit first argument pattern**

Methods that require at least one argument should use the signature pattern:

```php ignore
public function method(Type $first, Type ...$rest) : ReturnType
```

Rather than:

```php ignore
public function method(Type ...$args) : ReturnType
```

## Pros & Cons
---

**Advantages of the chosen pattern:**

- **Compile-time enforcement**: PHP enforces at least one argument at the language level
- **Static analysis support**: PHPStan, Psalm, and IDEs immediately understand the constraint
- **Better developer experience**: The method signature clearly communicates the requirement without needing to read documentation or encounter runtime exceptions
- **Fail-fast principle**: Errors are caught before execution, not during

**Disadvantages:**

- Internal implementation requires merging arguments: `array_unshift($rest, $first)`
- Slightly more verbose method signatures
- Minor performance overhead from array operations (negligible in practice)

## Alternatives Considered
---

### 1. Pure variadic with runtime validation

```php ignore
public function partitionBy(string|Reference ...$entries) : self
{
    if ([] === $entries) {
        throw new InvalidArgumentException('At least one entry is required.');
    }
    // ...
}
```

**Rejected because:** Developers only discover the constraint at runtime, leading to worse developer experience.

### 2. Validation in the References class

```php
final class References
{
    public function __construct(string|Reference ...$references)
    {
        if ([] === $references) {
            throw new InvalidArgumentException('References cannot be empty.');
        }
        // ...
    }
}
```

**Rejected because:** While this centralizes validation, it still defers the error to runtime. The explicit first argument pattern catches errors earlier in the development cycle.

## Links and References
---

- [GitHub Issue #2236](https://github.com/flow-php/flow/issues/2236) - Original proposal discussion
