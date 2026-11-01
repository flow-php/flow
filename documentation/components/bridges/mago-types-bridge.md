# Mago Types Bridge

[DOC_LINK:/documentation/components/libs/types]

[TOC]

This is an analyzer-plugin extension for [Mago](https://mago.carthage.software). It derives the array shape that a
`Flow\Types\DSL\type_structure()` call represents. Code that uses `assert()`ed values is then type-checked
against the real shape instead of `array<array-key, mixed>`.

It replaces the built-in `flow-php` plugin of Mago, and it adds a feature that the built-in plugin does not have:
support for the `structure_element()` marker. The member value type comes from the first type parameter of the
element. A literal `optional: true` (the `TOptional` template) makes the shape key optional.

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

Register the worker as an extension host in `mago.toml`, and enable the `flow/types` plugin instead
of the built-in `flow-php` plugin:

```toml
[extension-hosts.flow]
command = ["php", "vendor/flow-php/mago-types-bridge/bin/worker.php"]

[analyzer]
plugins = ["flow/types"]
```

Needs `carthage-software/mago` 1.47.4 or newer (the first release with extension hosts).

## What is derived

- `type_structure(['a' => type_integer()])`: `array{a: int}`. Each plain `Type` value is required.
- A `structure_element()` value with a literal `optional: true`: `a?:` (possibly undefined key).
- Anything not statically known (unsealed arrays, non-`Type` values): the
  derivation uses the native docblock and does not guess.
