---
package: flow-php/array-dot
---

# Array Dot

[PACKAGE_NAV]

[TOC]

Array Dot is a Flow PHP library that reads and changes array elements with dot notation. With dot notation, you can
go through nested arrays, for example configuration data or nested JSON objects, with one path string.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/array-dot.md).

## Available Functions

```php ignore
<?php 

array_dot_get(array $array, Path|string $path, ?Type $type = null) : mixed;
array_dot_set(array $array, Path|string $path, mixed $value) : array;
array_dot_rename(array $array, Path|string $path, string $newName) : array;
array_dot_exists(array $array, Path|string $path) : bool;
```

`array_dot_steps(string $path)` is deprecated. Use `Path::fromString($path)->steps`.

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

In the example above, `foo.bar.baz` is a path. Paths also support integer keys, for example
`foo.0.baz`.

`foo`, `bar` and `baz` are the single steps (keys) of the path.


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

Dot notation is strict by default. If a step of the path is not present,
the function throws an exception.

The `?` nullsafe operator changes this behavior.

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

You do not have to use nullsafe on the last step of the path.

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

The wildcard operator gives access to all paths in nested arrays.

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

The nullsafe wildcard operator gives access to all paths in nested arrays of collections that are not
symmetric.

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

Get only the selected keys from a nested array:

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

A backslash makes the next `\`, `.`, `?`, `*`, `,`, `{` or `}` part of the key. Before any other character, the
backslash stays in the key.

```php
<?php

$array = ['a.b' => ['*' => 1], '?x' => 2, 'k,l' => 3];

$value = array_dot_get($array, 'a\.b.\*'); // 1
$value = array_dot_get($array, '\?x'); // 2
$value = array_dot_get(['m' => $array], 'm.{\?x, k\,l}'); // ['?x' => 2, 'k,l' => 3]
```

### Path

Each function also accepts a `Flow\ArrayDot\Path` instead of a string. If the keys come from data, build the path from
steps. The keys are then never parsed:

```php
<?php

use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step\Key;
use Flow\ArrayDot\Step\Multimatch;
use Flow\ArrayDot\Step\Wildcard;

use function Flow\ArrayDot\array_dot_get;

$path = new Path([new Key('users'), new Wildcard(), new Multimatch([
    new Path([new Key('id')]),
    new Path([new Key('first.name', nullsafe: true)]),
])]);

$value = array_dot_get(['users' => [['id' => 1, 'first.name' => 'John'], ['id' => 2]]], $path);
// [['id' => 1, 'first.name' => 'John'], ['id' => 2, 'first.name' => null]]

$path->toString(); // 'users.*.{id,?first\.name}'
Path::fromString('users.*.{id,?first\.name}') == $path; // true
```
