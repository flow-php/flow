# Mago Types Bridge

[DOC_LINK:/documentation/components/libs/types]

[TOC]

A [Mago](https://mago.carthage.software) analyzer-plugin extension that derives the array shape a
`Flow\Types\DSL\type_structure()` call represents, so consuming `assert()`ed values type-checks
against the real shape instead of `array<array-key, mixed>`.

It replaces Mago's built-in `flow-php` plugin and adds what the built-in does not have:
`structure_element()` marker support - the member value type comes from the element's first type
parameter and a literal `optional: true` (the `TOptional` template) marks the shape key optional.

```php
use function Flow\Types\DSL\{type_structure, structure_element, type_integer, type_string};

$type = type_structure([
    'id' => type_integer(),
    'nick' => structure_element('nick', type_string(), optional: true),
    'name' => type_string(),
]);

// Derived: StructureType<array{id: int, nick?: string, name: string}>
$data = $type->assert($input);
$data['id'];           // int
$data['nick'] ?? null; // string|null - the key is possibly undefined
```

## Installation

```bash
composer require --dev flow-php/mago-types-bridge
```

## Configuration

Register the worker as an extension host in `mago.toml` and enable the `flow/types` plugin instead
of the built-in `flow-php` one:

```toml
[extension-hosts.flow]
command = ["php", "vendor/flow-php/mago-types-bridge/bin/worker.php"]

[analyzer]
plugins = ["flow/types"]
```

Requires `carthage-software/mago` 1.47.4 or newer (the first release with extension hosts).

## What is derived

- `type_structure(['a' => type_integer()])` - `array{a: int}`, every plain `Type` value required.
- A `structure_element()` value with a literal `optional: true` - `a?:` (possibly undefined key).
- A literal `allow_extra: true` - the shape stays open (`array-key => mixed` parameters).
- Anything not statically known (unsealed arrays, non-literal flags, non-`Type` values) - the
  derivation backs off to the native docblock instead of guessing.
