# Variadic Arguments Pattern for Required Parameters

[TOC]

Proposed by: @norberttech
Date: 2026-03-02

## Context
---

Several Flow PHP methods accept multiple arguments, and at least one argument is required. Examples:
- `DataFrame::partitionBy()`
- `Rows::partitionBy()`
- `Window::partitionBy()`
- Type comparison methods

A proposal wanted to make the API signatures simpler, from:

```php ignore
public function partitionBy(string|Reference $column, string|Reference ...$columns) : self
```

To:

```php ignore
public function partitionBy(string|Reference ...$columns) : self
```

The reason for the proposal was fewer array operations, such as the internal `array_unshift()` that merges the first required argument with the variadic rest.

## Decision
---

Keep the explicit first argument pattern.

Methods that require at least one argument must use this signature pattern:

```php ignore
public function method(Type $first, Type ...$rest) : ReturnType
```

Not:

```php ignore
public function method(Type ...$args) : ReturnType
```

## Pros & Cons
---

Advantages of the chosen pattern:

- Compile-time enforcement: PHP enforces at least one argument at the language level
- Static analysis support: PHPStan, Psalm, and IDEs understand the constraint immediately
- Better developer experience: the method signature shows the requirement clearly. The developer does not have to read documentation or get runtime exceptions
- Fail-fast principle: errors are found before execution, not during execution

Disadvantages:

- The internal implementation must merge arguments: `array_unshift($rest, $first)`
- Slightly longer method signatures
- A small performance overhead from array operations (negligible in practice)

## Alternatives Considered
---

### 1. Pure variadic with runtime validation

```php ignore
public function partitionBy(string|Reference ...$columns) : self
{
    if ([] === $columns) {
        throw new InvalidArgumentException('At least one column is required.');
    }
    // ...
}
```

Rejected because: developers find the constraint only at runtime. The developer experience is worse.

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

Rejected because: this puts the validation in one place, but the error still happens only at runtime. The explicit first argument pattern finds errors earlier in the development cycle.

## Links and References
---

- [GitHub Issue #2236](https://github.com/flow-php/flow/issues/2236): discussion of the original proposal
