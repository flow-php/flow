---
package: flow-php/phpstan-types-bridge
---

# PHPStan Types Bridge

PHPStan extension for [flow-php/types](/documentation/components/libs/types.md). It teaches PHPStan the
precise return type of the `Flow\Types\DSL\type_structure()` DSL function, so structure shapes are narrowed
for editor autocompletion and static analysis.

[PACKAGE_NAV]

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/phpstan-types-bridge.md).

## Overview

Without the extension PHPStan infers the return type of `type_structure()` as the generic `StructureType`.
With the extension installed, the structure definition is read from the call arguments and PHPStan narrows
the result to the concrete array shape:

```php
use function Flow\Types\DSL\{type_structure, type_integer, type_string};

// Inferred as: Type<array{id: int, name: string}>&StructureType
$schema = type_structure([
    'id' => type_integer(),
    'name' => type_string(),
]);

// Optional members (the second argument) become optional array-shape keys:
// Type<array{id: int, nickname?: string}>&StructureType
$schema = type_structure(
    ['id' => type_integer()],
    ['nickname' => type_string()],
);
```

The bridge also narrows `structure_element()` marker values inside the first argument - the member
type comes from the element's type and a literal `optional: true` flag turns the shape key optional:

```php
// Inferred as: Type<array{id: int, nickname?: string}>&StructureType
$schema = type_structure([
    'id' => type_integer(),
    'nickname' => structure_element('nickname', type_string(), optional: true),
]);
```

> **Note:** Flow PHP itself is analyzed with [Mago](https://mago.carthage.software/), whose `flow-php`
> plugin handles `type_structure()` natively. This bridge exists purely for downstream projects that use
> PHPStan.

## Requirements

- PHP 8.3+
- flow-php/types
- phpstan/phpstan ^2.1

## Usage

Register the extension in your `phpstan.neon` (skipped automatically when using
[phpstan/extension-installer](https://github.com/phpstan/extension-installer)):

```neon
includes:
    - vendor/flow-php/phpstan-types-bridge/extension.neon
```

That's it - `type_structure()` calls are now narrowed by PHPStan across your codebase.
