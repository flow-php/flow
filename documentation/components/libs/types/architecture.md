# Types - Architecture

Types is a small library designed to ensure type consistency at runtime.  
The concept is that wherever the system receives input data, one of three operations can be performed:

- `Type::isValid($value) : bool` - checks if the value is of the correct type
- `Type::cast($value) : mixed` - casts the value to a specific type, it must throw `\Flow\Types\Exception\CastingException` if the value
  cannot be casted to the expected type
- `Type::assert($value) : void` - checks if the value is of the correct type, throwing an `Flow\Types\Exception\InvalidTypeException` exception if not

Each operation is executed at runtime, not compile time, but their structure provides significant type information for
static code analysis tools.

By using these functions, static analysis tools can predict undesirable situations in the code, such as passing an
incorrect type to a function or method.

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

- Native Types - those built into PHP, e.g., `int`, `string`, `bool`, `float`, `array`, `object`.
- Logical Types - those that narrow and refine native types, e.g., `type_uuid()` is a refinement of `type_string()` and
  checks if a string is a valid UUID.

Additionally, all types are divided into:

- Composite Types - those composed of other types, e.g., `type_optional(type_string())` is equivalent to `?string`,
  i.e., a string or null.
- Simple Types - single types, e.g., `type_string()`, `type_int()`, `type_bool()`, `type_float()`, `type_array()`,
  `type_object()`.

## Architecture

The entire library is written following best object-oriented programming practices and design patterns. Each type is a
class implementing the `Flow\Types\Type` interface, enabling easy extension and addition of new types in the future.

To improve the developer experience, the library includes DSL (Domain Specific Language) functions that simplify
creating and using types in code.

For example, `new \Flow\Types\Type\Native\StringType()` is equivalent to `\Flow\Types\DSL\type_string()`.

This library also provides a set of helpers for working with types, that are also covered by the DSL.

For example, to determine a variable's type, you can use the `\Flow\Types\DSL\get_type($value) : Type` function, which
internally creates a new instance of `\Flow\Types\Type\TypeDetector` and calls its `detectType(mixed $value) : Type`
method.

The DSL definition can thus be considered the library's API, located in the
file [functions.php](src/lib/types/src/Flow/Types/DSL/functions.php).

One of the library's key principles is tight integration with static code analysis tools. This is achieved through
proper use of template mechanisms and type narrowing, `@template` and `@phpstan-assert`.

More details can be found at:

- [PHPStan Generic](https://phpstan.org/blog/generics-in-php-using-phpdocs)
- [Generic by Examples](https://phpstan.org/blog/generics-by-examples)
- [Narrowing Types](https://phpstan.org/writing-php-code/narrowing-types)

Therefore, it is critical that all classes and functions in this library are properly documented in PHPDoc.

## Testing

This library provides a set of unit tests that verify the correct operation of all types and DSL functions.  
All tests are located in the directory [`Flow/Types/Tests/Unit`](src/lib/types/tests/Flow/Types/Tests/Unit).

Below is an example template that can be used to create cover a type with unit tests:

```php
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
    
    public funciton test_to_string() : void
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


Additionally, if a given type provides any extra functions, they should be tested in separate unit tests, covering all
possible use cases.

The Types library is part of the Flow PHP framework for data processing.  
It is developed in a monorepository alongside other framework libraries and components.  
All tools, such as Composer, PHPUnit, PHPStan, Rector, and CS Fixer, are available in the same location.

Tests can be executed using the following command at the monorepo root level:

```php
composer test:lib:types
composer static:analyze
```
