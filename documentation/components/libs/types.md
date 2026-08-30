---
package: flow-php/types
---

# Types

Flow Types is a small library that provides a set of type classes for PHP. 
It's designed to work together with static analysis tools like PHPStan and Psalm. 

The main goal of this library is to simplify common type-related tasks, such as type checking, type casting, and type assertion.

[PACKAGE_NAV]

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/types.md).

### Usage

**Heads Up** - in order to use full potential of `type_structure()` with PHPStan, install the
[PHPStan Types Bridge](/documentation/components/bridges/phpstan-types-bridge.md):

```
composer require --dev flow-php/phpstan-types-bridge
```

With [phpstan/extension-installer](https://github.com/phpstan/extension-installer) it is registered
automatically, otherwise include it in your `phpstan.neon`:

```neon
includes:
    - vendor/flow-php/phpstan-types-bridge/extension.neon
```

#### Type Narrowing 

To narrow a variable to a specific type, you can use one of two available methods: 

- `Type::isValid(mixed $value) : bool`
- `Type::assert(mixed $value) : mixed`

The main difference between those two, is that isValid returns a boolean value, while assert will throw an exception if the type is not valid.

Examples:

```php
<?php

use Flow\Types\DSL\type_string;

$variable = $input->get('some-input');

$string = type_string()->assert($variable); 
```

The above code will throw an exception if the variable is not a string. 
On top of that it will also narrow the `$string` variable to a string type, so you can use it without any additional checks.

```php
<?php

use Flow\Types\DSL\type_string;

$variable = $input->get('some-input');

function doSomething(string $string): void
{
    // do something with string
}

if (type_string()->isValid($variable)) {
    doSomething($variable);
} 
```

The above code will check if the variable is a string, and if it is, it will call the `doSomething` function with the variable.
Thanks to the `isValid` method, you can use the variable without any additional checks and static analysis tools will not complain about it.

### Type Casting 

To cast a variable to a specific type, you just simply need to use the `cast` method:

> Note: The cast method will throw CastingException if the variable cannot be casted to the specified type.

> When passed value already has a valid type it's returned as is. 

```php
<?php

use Flow\Types\DSL\type_string;

$variable = $input->get('some-input');

$string = type_string()->cast($variable); 
```

Casting structures, lists and maps:

- a structure element that is absent, or present with `null`, throws `CastingException` when the element's type does
  not accept `null` - `getPrevious()` returns a `MissingElementCastingException` whose `element` property names the
  failing element
- elements whose type accepts `null` (`type_optional(...)`, `type_union(..., type_null())`) cast `null` to `null`;
  an absent optional element stays absent from the output
- `type_list(...)->cast(null)` and `type_map(...)->cast(null)` throw `CastingException`
- a JSON string payload is decoded and cast element-wise, exactly like an array payload

### Complex Types 

By default all types are not nullabe, in order to achieve nullability two types needs to be combined: 

- `type_optional(type_string())` - results in nullable string '?string'
- `type_union(type_string(),type_integer(),type_null())` - results in `union<string,integer,null>` which is the same as `string|integer|null`
- `type_intersection(type_integer(),type_string())` - results in `intersection<integer,string>` which is the same as `integer&string`, requiring values to be valid for ALL types in the intersection

#### Lists 

List is a collection of elements of the same where keys start from 0 and are auto incremented. 

```php
<?php

use Flow\Types\DSL\type_list;
use Flow\Types\DSL\type_string;

$listOfStrings = type_list(type_string());
```


#### Maps

Map is a key value data structure where all keys and all values has the same type. 

```php
<?php

use Flow\Types\DSL\type_map;
use Flow\Types\DSL\type_string;
use Flow\Types\DSL\type_integer;

$mapOfStringToInt = type_map(type_string(), type_integer());
```

#### Structures

Structure is an associative array with a defined shape. Field order is part of the type: two
structures with the same fields in a different order are different types.

```php
<?php

use Flow\Types\DSL\type_structure;
use Flow\Types\DSL\type_string;
use Flow\Types\DSL\type_integer;

$userStructure = type_structure([
    'id' => type_string(),
    'name' => type_string()
])
```

Insertion order of the `$elements` map is the field order. A plain `Type` value declares a required
field; a `structure_element()` value carries its own `optional` flag, so an optional field can sit
anywhere - including before a required one:

```php
<?php

use Flow\Types\DSL\structure_element;
use Flow\Types\DSL\type_structure;
use Flow\Types\DSL\type_string;

$userStructure = type_structure([
    'id' => type_string(),
    'nickname' => structure_element('nickname', type_string(), optional: true),
    'name' => type_string(),
]);
// structure{id: string, nickname?: string, name: string}
```

A `structure_element()` value must carry the same name as its key. Each element is a
`StructureElement` carrying its name, type, and `optional` flag; `StructureType::elements()`
returns them as one ordered list, and `StructureType::element($name)` looks a field up by name.

#### Combined Types 

All above types can be easily combined together, so for to get the list of users:

```php
<?php

use Flow\Types\DSL\type_list;
use Flow\Types\DSL\type_structure;
use Flow\Types\DSL\type_string;
use Flow\Types\DSL\type_integer;

$userStructure = type_list(
    type_structure([
        'id' => type_string(),
        'name' => type_string()
    ])
);
```
