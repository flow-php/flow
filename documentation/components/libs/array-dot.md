---
package: flow-php/array-dot
---

# Array Dot

[PACKAGE_NAV]

[TOC]

Flow PHP's Array Dot is a proficient library engineered to enhance array handling and manipulation in PHP. This library
embodies a practical solution for accessing and manipulating array elements using dot notation, facilitating a more
readable and maintainable code base. By leveraging the dot notation, developers can effortlessly traverse nested arrays
and perform operations on array elements with ease and precision. Flow PHP's Array Dot library encapsulates the
intricacies of array manipulation, offering a simplified yet powerful API that caters to both simple and complex array
operations. This library aligns well with Flow PHP's core ethos of efficient data processing and transformation, making
it a valuable asset for developers aiming to streamline their array handling tasks in PHP. Whether dealing with
configuration data, nested JSON objects, or any other complex array structures, the Array Dot library is a reliable
companion for achieving cleaner and more efficient array operations.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/array-dot.md).

## Available Functions

```php ignore
<?php 

array_dot_get(array $array, Path|string $path, ?Type $type = null) : mixed;
array_dot_set(array $array, Path|string $path, mixed $value) : array;
array_dot_rename(array $array, Path|string $path, string $newName) : array;
array_dot_exists(array $array, Path|string $path) : bool;
```

`array_dot_steps(string $path)` is deprecated, use `Path::fromString($path)->steps`.

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

$value = array_dot_get($array, 'foo.bar.baz'); // 1000

$array = array_dot_set([], 'foo.bar.baz', 1000); // ['foo' => ['bar' => ['baz' => 1000]]];
```

In above example `foo.bar.baz` is path which also supports integer keys. For example
`foo.0.baz`.

`foo`, `bar`, `baz` represents single steps (keys) of path.


### Dot Notation - Custom Operators

- `?` - nullsafe
- `*` - wildcard
- `?*` - nullsafe wildcard


### Dot Notation - Custom Syntax

- `{}` - multipath

#### Nullsafe Operator - ?

Supported in functions:

- `array_dot_get`
- `array_dot_exists`
- `array_dot_rename` - an absent key is left as it is

`array_dot_set` writes the key without the `?`.

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

$value = array_dot_get($array, 'foo.bar.nothing'); // InvalidPathException
$value = array_dot_get($array, 'foo.bar.?nothing'); // null
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

$value = array_dot_get($array, 'foo.?bar.nothing'); // null
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

$value = array_dot_get($array, 'users.*.id'); // [1, 2]
```

#### Nullsafe Wildcard Operator - ?*

Supported in functions:

- `array_dot_get`
- `array_dot_rename` - elements without the key are left as they are

Nullsafe Wildcard operator allows to access all paths in nested arrays for non symmetric
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

$value = array_dot_get($array, 'users.?*.name'); // ['John']
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

$value = array_dot_get($array, 'users.*.{id,?role}'); // [['id' => 1, 'role' => null], ['id' => 2, 'role' => 'ADMIN']]
```

### Dot Notation - Escaping

A backslash makes the next `\`, `.`, `?`, `*`, `,`, `{` or `}` part of the key. Before any other character the
backslash stays in the key.

```php
<?php

$array = ['a.b' => ['*' => 1], '?x' => 2, 'k,l' => 3];

$value = array_dot_get($array, 'a\.b.\*'); // 1
$value = array_dot_get($array, '\?x'); // 2
$value = array_dot_get(['m' => $array], 'm.{\?x, k\,l}'); // ['?x' => 2, 'k,l' => 3]
```

### Path

Every function accepts a `Flow\ArrayDot\Path` instead of a string. Build it from steps when the keys come from data,
so they are never parsed:

```php
<?php

use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step\Key;
use Flow\ArrayDot\Step\Multimatch;
use Flow\ArrayDot\Step\Wildcard;

$path = new Path([new Key('users'), new Wildcard(), new Multimatch([
    new Path([new Key('id')]),
    new Path([new Key('first.name', nullsafe: true)]),
])]);

$value = array_dot_get(['users' => [['id' => 1, 'first.name' => 'John'], ['id' => 2]]], $path);
// [['id' => 1, 'first.name' => 'John'], ['id' => 2, 'first.name' => null]]

$path->toString(); // 'users.*.{id,?first\.name}'
Path::fromString('users.*.{id,?first\.name}') == $path; // true
```
