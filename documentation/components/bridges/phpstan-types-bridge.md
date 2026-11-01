---
package: flow-php/phpstan-types-bridge
---

# PHPStan Types Bridge

PHPStan extension for [flow-php/types](/documentation/components/libs/types.md). It gives PHPStan the
precise return type of the `Flow\Types\DSL\type_structure()` DSL function. Structure shapes are therefore narrowed
for editor autocompletion and static analysis.

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/phpstan-types-bridge.md).

## Overview

Without the extension, PHPStan infers the return type of `type_structure()` as the generic `StructureType`.
With the extension, PHPStan reads the structure definition from the call arguments and narrows
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

The bridge also narrows `structure_element()` marker values inside the first argument. The member
type comes from the type of the element. A literal `optional: true` flag makes the shape key optional:

```php
// Inferred as: Type<array{id: int, nickname?: string}>&StructureType
$schema = type_structure([
    'id' => type_integer(),
    'nickname' => structure_element('nickname', type_string(), optional: true),
]);
```

> Note: Flow PHP itself is analyzed with [Mago](https://mago.carthage.software/). Its `flow-php`
> plugin handles `type_structure()` natively. This bridge exists only for downstream projects that use
> PHPStan.

## Requirements

- PHP 8.3+
- flow-php/types
- phpstan/phpstan ^2.1

## Usage

Register the extension in your `phpstan.neon`. With
[phpstan/extension-installer](https://github.com/phpstan/extension-installer), this step is automatic:

```neon
includes:
    - vendor/flow-php/phpstan-types-bridge/extension.neon
```

PHPStan now narrows the `type_structure()` calls in your full codebase.
