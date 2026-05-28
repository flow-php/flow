# Flow PHP - PHPStan Types Bridge

PHPStan extension for [flow-php/types](https://github.com/flow-php/types). It teaches PHPStan the precise
return type of the `Flow\Types\DSL\type_structure()` function so that structure shapes are narrowed for
autocompletion and static analysis.

## Installation

```bash
composer require --dev flow-php/phpstan-types-bridge
```

If you use [phpstan/extension-installer](https://github.com/phpstan/extension-installer) the extension is
registered automatically. Otherwise include it manually in your `phpstan.neon`:

```neon
includes:
    - vendor/flow-php/phpstan-types-bridge/extension.neon
```

## Documentation

- [Documentation](https://github.com/flow-php/flow/blob/1.x/documentation/components/bridges/phpstan-types-bridge.md)
- [Contributing](https://github.com/flow-php/flow/blob/1.x/CONTRIBUTING.md)
