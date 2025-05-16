# Types

Flow Types is a small library that provides a set of type classes for PHP. 
It's designed to work together with static analysis tools like PHPStan and Psalm. 

The main goal of this library is to simplify common type-related tasks, such as type checking, type casting, and type assertion.

- [⬅️️ Back](../../introduction.md)

## Installation

```
composer require flow-php/types:~--FLOW_PHP_VERSION--
```

### Usage

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


### Complex Types 

By default all types are not nullabe, in order to achieve nullability two types needs to be combined: 

- `type_optional(type_string())` - results in nullable string '?string'
- `type_union(type_string(),type_integer(),type_null())` - results in `union<string,integer,null>` which is the same as `string|integer|null`

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

Structure is a associative array with a defined shape. 

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