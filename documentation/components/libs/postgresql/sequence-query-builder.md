# Sequence Query Builder

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The Sequence Query Builder provides a fluent, type-safe interface for constructing PostgreSQL sequence management statements:
CREATE SEQUENCE, ALTER SEQUENCE, and DROP SEQUENCE.

## CREATE SEQUENCE

### Basic Sequence Creation

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->sequence('user_id_seq');

echo $query->toSql();
// CREATE SEQUENCE user_id_seq
```

### With Schema

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->sequence('user_id_seq', 'public');

echo $query->toSql();
// CREATE SEQUENCE public.user_id_seq
```

### IF NOT EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->sequence('user_id_seq')->ifNotExists();

echo $query->toSql();
// CREATE SEQUENCE IF NOT EXISTS user_id_seq
```

### Temporary Sequence

Create a temporary sequence that is automatically dropped at the end of the session:

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->sequence('temp_seq')->temporary();

echo $query->toSql();
// CREATE TEMPORARY SEQUENCE temp_seq
```

### Unlogged Sequence

Create an unlogged sequence for better performance (data not written to WAL):

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->sequence('fast_seq')->unlogged();

echo $query->toSql();
// CREATE UNLOGGED SEQUENCE fast_seq
```

### START WITH

Specify the starting value of the sequence:

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->sequence('user_id_seq')
    ->startWith(100);

echo $query->toSql();
// CREATE SEQUENCE user_id_seq START 100
```

### INCREMENT BY

Specify the increment value (positive for ascending, negative for descending):

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->sequence('user_id_seq')
    ->incrementBy(10);

echo $query->toSql();
// CREATE SEQUENCE user_id_seq INCREMENT 10
```

### MINVALUE / NO MINVALUE

Set the minimum value or remove the minimum bound:

```php
<?php

use function Flow\PostgreSql\DSL\create;

// Set minimum value
$query = create()->sequence('user_id_seq')
    ->minValue(1);

echo $query->toSql();
// CREATE SEQUENCE user_id_seq MINVALUE 1

// Remove minimum bound
$query = create()->sequence('user_id_seq')
    ->noMinValue();

echo $query->toSql();
// CREATE SEQUENCE user_id_seq NO MINVALUE
```

### MAXVALUE / NO MAXVALUE

Set the maximum value or remove the maximum bound:

```php
<?php

use function Flow\PostgreSql\DSL\create;

// Set maximum value
$query = create()->sequence('user_id_seq')
    ->maxValue(9999999);

echo $query->toSql();
// CREATE SEQUENCE user_id_seq MAXVALUE 9999999

// Remove maximum bound
$query = create()->sequence('user_id_seq')
    ->noMaxValue();

echo $query->toSql();
// CREATE SEQUENCE user_id_seq NO MAXVALUE
```

### CACHE

Specify how many sequence numbers are preallocated and stored in memory:

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->sequence('user_id_seq')
    ->cache(20);

echo $query->toSql();
// CREATE SEQUENCE user_id_seq CACHE 20
```

### CYCLE / NO CYCLE

Enable or disable cycling when the sequence reaches its bounds:

```php
<?php

use function Flow\PostgreSql\DSL\create;

// Enable cycling
$query = create()->sequence('user_id_seq')
    ->cycle();

echo $query->toSql();
// CREATE SEQUENCE user_id_seq CYCLE

// Disable cycling (explicit)
$query = create()->sequence('user_id_seq')
    ->noCycle();

echo $query->toSql();
// CREATE SEQUENCE user_id_seq NO CYCLE
```

### AS (Data Type)

Specify the data type of the sequence (smallint, integer, or bigint):

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->sequence('user_id_seq')
    ->asType('bigint');

echo $query->toSql();
// CREATE SEQUENCE user_id_seq AS bigint
```

### OWNED BY

Associate the sequence with a table column (dropped automatically when the column is dropped):

```php
<?php

use function Flow\PostgreSql\DSL\create;

// Basic owned by
$query = create()->sequence('user_id_seq')
    ->ownedBy('users', 'id');

echo $query->toSql();
// CREATE SEQUENCE user_id_seq OWNED BY users.id

// With schema-qualified table
$query = create()->sequence('user_id_seq')
    ->ownedBy('public.users', 'id');

echo $query->toSql();
// CREATE SEQUENCE user_id_seq OWNED BY public.users.id

// Remove ownership
$query = create()->sequence('user_id_seq')
    ->ownedByNone();

echo $query->toSql();
// CREATE SEQUENCE user_id_seq OWNED BY "none"
```

### Complete Example

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->sequence('user_id_seq')
    ->asType('bigint')
    ->startWith(1)
    ->incrementBy(1)
    ->minValue(1)
    ->maxValue(1000000)
    ->cache(1)
    ->noCycle();

echo $query->toSql();
// CREATE SEQUENCE user_id_seq AS bigint START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 1000000 CACHE 1 NO CYCLE
```

## ALTER SEQUENCE

### RESTART

Restart the sequence at its start value or at a specific value:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

// Restart at start value
$query = alter()->sequence('user_id_seq')
    ->restart();

echo $query->toSql();
// ALTER SEQUENCE user_id_seq RESTART

// Restart at specific value
$query = alter()->sequence('user_id_seq')
    ->restartWith(1000);

echo $query->toSql();
// ALTER SEQUENCE user_id_seq RESTART 1000
```

### IF EXISTS

Only alter the sequence if it exists:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->sequence('user_id_seq')
    ->withIfExists()
    ->incrementBy(10);

echo $query->toSql();
// ALTER SEQUENCE IF EXISTS user_id_seq INCREMENT 10
```

### With Schema

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->sequence('user_id_seq', 'public')
    ->incrementBy(10);

echo $query->toSql();
// ALTER SEQUENCE public.user_id_seq INCREMENT 10
```

### Modify Options

All CREATE SEQUENCE options can also be modified with ALTER SEQUENCE:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->sequence('user_id_seq')
    ->incrementBy(10)
    ->minValue(1)
    ->maxValue(1000000)
    ->cache(5);

echo $query->toSql();
// ALTER SEQUENCE user_id_seq INCREMENT 10 MINVALUE 1 MAXVALUE 1000000 CACHE 5
```

### RENAME TO

Rename a sequence:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->sequence('old_seq')
    ->renameTo('new_seq');

echo $query->toSql();
// ALTER SEQUENCE old_seq RENAME TO new_seq
```

### SET SCHEMA

Move a sequence to a different schema:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->sequence('user_id_seq')
    ->setSchema('new_schema');

echo $query->toSql();
// ALTER SEQUENCE user_id_seq SET SCHEMA new_schema
```

### OWNER TO

Change the owner of a sequence:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->sequence('user_id_seq')
    ->ownerTo('new_owner');

echo $query->toSql();
// ALTER SEQUENCE user_id_seq OWNER TO new_owner
```

### SET LOGGED / SET UNLOGGED

Change the logging behavior of a sequence:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

// Make the sequence logged
$query = alter()->sequence('user_id_seq')
    ->setLogged();

echo $query->toSql();
// ALTER SEQUENCE user_id_seq SET LOGGED

// Make the sequence unlogged
$query = alter()->sequence('user_id_seq')
    ->setUnlogged();

echo $query->toSql();
// ALTER SEQUENCE user_id_seq SET UNLOGGED
```

## DROP SEQUENCE

### Simple Drop

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->sequence('user_id_seq');

echo $query->toSql();
// DROP SEQUENCE user_id_seq
```

### With Schema

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->sequence('public.user_id_seq');

echo $query->toSql();
// DROP SEQUENCE public.user_id_seq
```

### IF EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->sequence('user_id_seq')->ifExists();

echo $query->toSql();
// DROP SEQUENCE IF EXISTS user_id_seq
```

### Multiple Sequences

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->sequence('user_id_seq', 'order_id_seq', 'product_id_seq');

echo $query->toSql();
// DROP SEQUENCE user_id_seq, order_id_seq, product_id_seq
```

### CASCADE

Drop objects that depend on the sequence:

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->sequence('user_id_seq')
    ->cascade();

echo $query->toSql();
// DROP SEQUENCE user_id_seq CASCADE
```

### RESTRICT

Refuse to drop the sequence if any objects depend on it (default behavior):

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->sequence('user_id_seq')
    ->restrict();

echo $query->toSql();
// DROP SEQUENCE user_id_seq
```

### Combined Options

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->sequence('user_id_seq')
    ->ifExists()
    ->cascade();

echo $query->toSql();
// DROP SEQUENCE IF EXISTS user_id_seq CASCADE
```

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/postgresql/namespaces/flow-postgresql-dsl.html).
