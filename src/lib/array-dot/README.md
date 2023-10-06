# Array Dot

Flow PHP's Array Dot is a proficient library engineered to enhance array handling and manipulation in PHP. This library
embodies a practical solution for accessing and manipulating array elements using dot notation, facilitating a more
readable and maintainable code base. By leveraging the dot notation, developers can effortlessly traverse nested arrays
and perform operations on array elements with ease and precision. Flow PHP's Array Dot library encapsulates the
intricacies of array manipulation, offering a simplified yet powerful API that caters to both simple and complex array
operations. This library aligns well with Flow PHP's core ethos of efficient data processing and transformation, making
it a valuable asset for developers aiming to streamline their array handling tasks in PHP. Whether dealing with
configuration data, nested JSON objects, or any other complex array structures, the Array Dot library is a reliable
companion for achieving cleaner and more efficient array operations.

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.

## Installation

```
composer require flow-php/array-dot:1.x@dev
```

## Available Functions

```php
<?php 

array_dot_get(array $array, string $path) : mixed;
array_dot_set(array $array, string $path, mixed $value) : mixed;
array_dot_rename(array $array, string $path, string $newName) : mixed;
array_dot_exists(array $array, string $path) : bool;
array_dot_steps(string $path) : array;
```

### Dot Notation - Basic Syntax

```php
<?php 

$array = [
    'foo' => [
        'bar' => [
            'baz' => 1000
        ]
    ]
];

$value = array_dot_get('foo.bar.baz'); // 1000

$array = array_dot_set([], 'foo.bar.baz', 1000); // ['foo' => ['bar' => ['baz' => 1000]]];
```

In above example `foo.bar.baz` is path which also supports integer keys. For exmaple
`foo.0.baz`.

`foo`, `bar`, `baz` represents single steps (keys) of path. 


### Dot Notation - Custom Operators

- `?` - nullsafe
- `*` - wildcard
- `?*` - nullsafe wildcar


### Dot Notation - Custom Syntax

- `{}` - multipath

#### Nullsafe Operator - ? 

Supported in functions:

- `array_dot_get`

Dot notation is strict by default, which means that if any step of path is not present,
function will throw exception. 

This behavior can be changed by `?` nullsafe operator. 

```php
<?php 

$array = [
    'foo' => [
        'bar' => [
            'baz' => 1000
        ]
    ]
];

$value = array_dot_get('foo.bar.nothing'); // InvalidPathException
$value = array_dot_get('foo.bar.?nothing'); // null
```

Nullsafe does not need to be used with the last step of path.

```php
<?php 

$array = [
    'foo' => [
        'fii' => [
            'oop' => 1000
        ]
    ]
];

$value = array_dot_get('foo.?bar.nothing'); // null
```

#### Wildcard Operator - *

Supported in functions:

- `array_dot_get`
- `array_dot_set`
- `array_dot_rename`

Wildcard operator allows to access all paths in nested arrays.

```php
<?php 

$array = [
    'users' => [
        [
            'id' => 1
        ],
        [
            'id' => 2
        ],
    ]
];

$value = array_dot_get('foo.*.id'); // [1, 2]
```

#### Nullsafe Wildcard Operator - ?*

Supported in functions:

- `array_dot_get`

Nullsafe Wildcard operator allows to access all paths in nested arrays for non symetric
collections.

```php
<?php 

$array = [
    'users' => [
        [
            'id' => 1,
            'name' => 'John'
        ],
        [
            'id' => 2
        ],
    ]
];

$value = array_dot_get('foo.*.name'); // ['John']
```

#### Multipath Syntax - {}

Supported in functions:

- `array_dot_get`

Get only selected keys from nested array

```php
<?php 

$array = [
    'users' => [
        [
            'id' => 1,
            'name' => 'John',
            'status' => 'active',
        ],
        [
            'id' => 2,
            'name' => 'Mikel',
            'status' => 'active',
            'role' => 'ADMIN'
        ],
    ]
];

$value = array_dot_get('foo.*.{id,?role}'); // [[1, null], [2, 'ADMIN']]
```


## Development

In order to install dependencies please, launch following commands:

```bash
composer install
composer install --working-dir ./tools
```

## Run Tests

In order to execute full test suite, please launch following command:

```bash
composer build
```

It's recommended to use [pcov](https://pecl.php.net/package/pcov) for code coverage however you can also use
xdebug by setting `XDEBUG_MODE=coverage` env variable.
