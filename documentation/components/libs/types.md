---
package: flow-php/types
---

# Types

Flow Types is a small library with type classes for PHP.
It works together with static analysis tools such as PHPStan and Psalm.

The library makes common type tasks simpler: type checks, type casts and type assertions.

[PACKAGE_NAV]

[TOC]

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/types.md).

### Usage

To get all features of `type_structure()` with PHPStan, install the
[PHPStan Types Bridge](/documentation/components/bridges/phpstan-types-bridge.md):

```
composer require --dev flow-php/phpstan-types-bridge
```

[phpstan/extension-installer](https://github.com/phpstan/extension-installer) registers it automatically.
Without the installer, include it in your `phpstan.neon`:

```neon
includes:
    - vendor/flow-php/phpstan-types-bridge/extension.neon
```

#### Type Narrowing 

To narrow a variable to a specific type, use one of these two methods:

- `Type::isValid(mixed $value) : bool`
- `Type::assert(mixed $value) : mixed`

The difference: isValid returns a boolean value. For a type that is not valid, assert throws an exception.

Examples:

```php
<?php

use function Flow\Types\DSL\type_string;

$variable = $input->get('some-input');

$string = type_string()->assert($variable); 
```

For a variable that is not a string, the code above throws an exception.
It also narrows the `$string` variable to a string type, so you can use it without more checks.

```php
<?php

use function Flow\Types\DSL\type_string;

$variable = $input->get('some-input');

function doSomething(string $string): void
{
    // do something with string
}

if (type_string()->isValid($variable)) {
    doSomething($variable);
} 
```

For a variable that is a string, the code above calls the `doSomething` function with the variable.
Because of the `isValid` method, you can use the variable without more checks, and static analysis tools do not report it.

### Type Casting 

To cast a variable to a specific type, use the `cast` method:

> Note: for a variable that it cannot cast to the specified type, the cast method throws CastingException.

> A value that already has a valid type comes back unchanged.

```php
<?php

use function Flow\Types\DSL\type_string;

$variable = $input->get('some-input');

$string = type_string()->cast($variable); 
```

The rule is: parse the text as a number, then narrow. The only values that `cast()` refuses are
values that the target cannot represent at all. It never refuses a value only because of lost precision. Lost precision is not a refusal:
`type_integer()->cast(1.5)` is `1`, `type_integer()->cast('12.9')` is `12`, `type_integer()->cast('1e5')`
is `100000`, and `type_boolean()->cast('on')` is `true`. `cast()` refuses a conversion that puts a different value
in place of the input:

- An integer literal that does not fit. `type_integer()->cast('9223372036854775808')` and
  `type_integer()->cast(1e300)` throw. They do not saturate to `PHP_INT_MAX` or collapse to `0`.
  The boundaries themselves still cast: `'9223372036854775807'`, `'-9223372036854775808'`.
- A date string that is not a calendar date. `type_date()->cast('')`, `type_datetime()->cast('now')`
  and `type_datetime()->cast('+12')` throw. They do not resolve against the wall clock, because then the same input
  gives a different value on each run. A date-only string still casts under `type_datetime()`, to midnight. 8-digit
  compact ISO (`'20240305'`) is also accepted. Numeric Unix timestamps do not change:
  `type_datetime()->cast(1700000000)` still works.
- `null` into a string. `type_string()->cast(null)` throws. It does not return `''`.
- A scalar into a container. `type_array()->cast('abc')` and `type_list(type_integer())->cast(5)`
  throw, and the message names the fix (`wrap the value first, e.g. type_list(type_string())`).
  `type_array()->cast(null)` also throws. It does not make an empty array.

A `datetime` type has one time zone. The default is UTC. Each value that it casts goes into that zone, and the
instant does not change:

```php
type_datetime()->cast('2026-01-02 03:04');                                 // 2026-01-02 03:04:00 UTC, date.timezone ignored
type_datetime('Europe/Warsaw')->cast('2026-01-02T03:04:05Z');               // 2026-01-02 04:04:05 Europe/Warsaw
type_datetime('Europe/Warsaw')->cast('2026-03-29 02:30:00');                // 2026-03-29 03:30:00 +02:00 - a DST gap moves forward
type_datetime('Europe/Warsaw')->cast('2026-10-25 02:30:00');                // +01:00 - an overlap takes the later occurrence...
type_datetime('America/New_York')->cast('2026-11-01 01:30:00');             // -04:00 - ...or the earlier one, west of UTC
type_datetime('America/New_York')->cast(type_date()->cast('2026-01-02'));   // 2026-01-01 19:00 - a date is midnight UTC
type_equals(type_datetime(), type_datetime('+02:00'));                      // false
```

A zone is an IANA region (`Europe/Warsaw`), `UTC` or an offset (`+05:30`). Abbreviations such as `CET` or `PST` are
refused, because PHP reads them as fixed offsets without daylight-saving rules.

`DateTimeInterface` and `DateInterval` convert to `int`/`float` in seconds, so they convert back
with `type_datetime()->cast(<int>)`. `float` keeps the sub-second fraction. `int` rounds down to whole
seconds.

Casting structures, lists and maps:

- The type of a structure element can refuse `null`. If such an element is absent or `null`, the cast throws
  `CastingException`. `getPrevious()` returns a `MissingElementCastingException`. Its `element` property names the
  element that failed.
- Elements with a type that accepts `null` (`type_optional(...)`, `type_union(..., type_null())`) cast `null` to
  `null`. An absent optional element stays absent from the output.
- The element type of a list or map can refuse `null`. A `null` element in such a list or map throws
  `CastingException`. Before, `type_list(type_string())->cast([null])` gave `['']`.
- `type_list(...)->cast(null)` and `type_map(...)->cast(null)` throw `CastingException`.
- A scalar is never wrapped into a single-item list. `type_list(type_string())->cast('hello')` throws. It does not
  return `['hello']`.
- A JSON string payload is decoded and cast element by element, exactly like an array payload.

### Complex Types 

By default, no type is nullable. For a nullable type, combine two types:

- `type_optional(type_string())` - results in nullable string '?string'
- `type_union(type_string(),type_integer(),type_null())` - results in `union<string,integer,null>` which is the same as `string|integer|null`
- `type_intersection(type_integer(),type_string())` - results in `intersection<integer,string>` which is the same as `integer&string`. A value must be valid for all types in the intersection

#### Lists 

A list is a collection of elements of the same type. The keys start at 0 and increase by 1.

```php
<?php

use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

$listOfStrings = type_list(type_string());
```


#### Maps

A map is a key-value data structure. All keys have the same type, and all values have the same type.

```php
<?php

use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_integer;

$mapOfStringToInt = type_map(type_string(), type_integer());
```

#### Structures

A structure is an associative array with a defined shape. The field order is part of the type. Two
structures with the same fields in a different order are different types.

```php
<?php

use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_integer;

$userStructure = type_structure([
    'id' => type_string(),
    'name' => type_string()
]);
```

The insertion order of the `$elements` map is the field order. A plain `Type` value declares a required
field. A `structure_element()` value has its own `optional` flag. An optional field can therefore be at any
position, also before a required field:

```php
<?php

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_string;

$userStructure = type_structure([
    'id' => type_string(),
    'nickname' => structure_element('nickname', type_string(), optional: true),
    'name' => type_string(),
]);
// structure{id: string, nickname?: string, name: string}
```

A `structure_element()` value must have the same name as its key. Each element is a
`StructureElement` with its name, type and `optional` flag. `StructureType::elements()`
returns them as one ordered list. `StructureType::element($name)` finds a field by name.

#### Combined Types 

You can combine all types above. For example, a list of users:

```php
<?php

use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_integer;

$userStructure = type_list(
    type_structure([
        'id' => type_string(),
        'name' => type_string()
    ])
);
```
