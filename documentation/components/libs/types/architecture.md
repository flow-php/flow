# Types - Architecture

[DOC_LINK:/documentation/components/libs/types]

[TOC]

Types is a small library that keeps types consistent at runtime.
The idea: each place where the system receives input data can do one of three operations:

- `Type::isValid($value) : bool` returns true for a value of the correct type.
- `Type::cast($value) : mixed` casts the value to a specific type. For a value that it cannot cast to the expected
  type, it must throw `\Flow\Types\Exception\CastingException`.
- `Type::assert($value) : mixed` returns a value of the correct type. For any other value, it throws
  `Flow\Types\Exception\InvalidTypeException`.

Each operation runs at runtime, not at compile time. But their structure gives much type information to static code
analysis tools.

With these functions, static analysis tools can find problems in the code before it runs. An example is an
incorrect type that goes to a function or method.

```php
<?php

$userInput = $input->get('value'); // we expect this value to be an integer

if (\Flow\Types\DSL\type_integer()->isValid($userInput)) {
  // at this point static analysis knows that $userInput is an integer
} 

// make that value an int 
$int = Flow\Types\DSL\type_integer()->cast($userInput);

// check if value is an integer, throws an exception if not
$int = Flow\Types\DSL\type_integer()->assert($userInput);
```

## Building Blocks

The main building block of this library is the interface [`Flow\Types\Type`](/src/lib/types/src/Flow/Types/Type.php).

This interface is implemented by two kinds of types:

- Native Types: the types of PHP itself, for example `int`, `string`, `bool`, `float`, `array`, `object`.
- Logical Types: types that make native types narrower. For example, `type_uuid()` is a narrower `type_string()`. It
  accepts only a string that is a valid UUID.

All types are also in one of these groups:

- Composite Types: types made of other types. For example, `type_optional(type_string())` is the same as `?string`,
  that is, a string or null.
- Simple Types: single types, for example `type_string()`, `type_integer()`, `type_boolean()`, `type_float()`,
  `type_array()`, `type_object()`.

## Architecture

The library follows object-oriented programming practices and design patterns. Each type is a class that implements
the `Flow\Types\Type` interface. You can therefore extend the library and add new types.

For a better developer experience, the library has DSL (Domain Specific Language) functions. They make it simpler to
create and use types in code.

For example, `new \Flow\Types\Type\Native\StringType()` is the same as `\Flow\Types\DSL\type_string()`.

This library also has helpers for types, and the DSL also covers them.

For example, to find the type of a variable, use the `\Flow\Types\DSL\get_type($value) : Type` function. Internally,
it creates a new instance of `\Flow\Types\Type\TypeDetector` and calls its `detectType(mixed $value) : Type`
method.

The DSL definition is therefore the API of the library. It is in the
file [functions.php](/src/lib/types/src/Flow/Types/DSL/functions.php).

One main principle of the library is tight integration with static code analysis tools. The library gets this
through correct use of templates and type narrowing, `@template` and `@assert`.

For more information, read:

- [PHPStan Generic](https://phpstan.org/blog/generics-in-php-using-phpdocs)
- [Generic by Examples](https://phpstan.org/blog/generics-by-examples)
- [Narrowing Types](https://phpstan.org/writing-php-code/narrowing-types)

All classes and functions in this library must therefore have correct PHPDoc.

Two rules follow from that. Both rules are necessary for the analysis, not a matter of style.

The first rule: if the type of an implementation is narrower than its native signature, the implementation repeats
`@return`. Mago
reads a return type from the docblock of the implementation, never from the docblock of the interface. An
implementation can declare `assert(mixed $value): string` without `@return non-empty-string`. It then returns
`string` without a warning, and it loses exactly the narrowing that the library exists for. Mago reports no
diagnostic for this: the program is valid, and nothing looks at an inferred type.
[`TypeNarrowing`](/src/lib/types/tests/Flow/Types/Tests/Unit/Type/Fixtures/TypeNarrowing.php) sends each `type_*()`
through `assert()`, `cast()` and `isValid()` into sinks that accept only the narrow type. `just analyze` therefore
fails on the line that loses a narrowing.

The second rule: each implementation declares `@template T of X` and `@implements Type<T>`. This also applies to an
implementation where nothing can bind `T` to a type other than its own bound. These templates look unnecessary, but they are necessary.
The `flow-php` analyzer plugin of Mago builds a `type_structure()` shape. It reads type parameter 0 from the concrete
class of each element. If an element class has no type parameters, the plugin discards the full shape and makes it
`array<array-key, mixed>`. Again, there is no diagnostic.
[`StructureShapeInference`](/src/lib/types/tests/Flow/Types/Tests/Unit/Type/Fixtures/StructureShapeInference.php)
pins those shapes. If this rule breaks, `just analyze` fails. The derivation runs in the `flow/types` Mago analyzer
plugin. [flow-php/mago-types-bridge](/src/bridge/mago/types) ships this plugin, and `mago.toml` configures it. It
replaces the built-in `flow-php` plugin of Mago. It does the same `type_structure()` shape derivation, and it also
supports the `structure_element()` marker. The member value type comes from the first template of the element. A
literal `optional: true` (the `TOptional` template) makes the shape key optional.
`StructureShapeInference::markerElements` pins the marker derivation.

## Testing

This library has unit tests that make sure that all types and DSL functions work correctly.
All tests are in the directory [`Flow/Types/Tests/Unit`](/src/lib/types/tests/Flow/Types/Tests/Unit).

This example template covers a type with unit tests:

```php ignore
<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use PHPUnit\Framework\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use Flow\Types\Exception\InvalidTypeException;

final class CustomTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        // since assert is based on isValid() we should the same cases as for isValid() but we shold expect it to throw an exception.
    }

    public static function cast_data_provider() : \Generator
    {
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        // we want to check if for given output method cast() returns expected value or throws an exception when given value can't be casted
    }

    public static function is_valid_data_provider() : \Generator
    {
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        // we want to check if for given output method isValid() returns true or false
    }
    
    public function test_to_string() : void
    {
        // we want to check the output of toString() method for type
    }
    
    public function test_normalization() : void
    {
        // we want to use normalize() to turn type into an array representation
        // then we want to use \Flow\Types\DSL\type_from_array() to create a new type from that array and compare it with the original type
    }
}
```


If a type has extra functions, test them in separate unit tests that cover all use cases.

The Types library is part of the Flow PHP framework for data processing.
Its development happens in a monorepository, together with the other libraries and components of the framework.
All tools, such as Composer, PHPUnit and Mago, are in the same location.

To run the tests, use these commands in the root of the monorepo:

```shell
just test --testsuite=lib-types-unit
just lint
just analyze
```
