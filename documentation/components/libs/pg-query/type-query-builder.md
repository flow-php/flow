# Type Query Builder

- [Back](/documentation/components/libs/pg-query.md)

[TOC]

The Type Query Builder provides a fluent, type-safe interface for constructing PostgreSQL type management statements:
CREATE TYPE (composite, enum, range), ALTER TYPE, and DROP TYPE.

## CREATE TYPE (Composite)

### Basic Composite Type

```php
<?php

use function Flow\PgQuery\DSL\{create_composite_type, type_attr};

$query = create_composite_type('address')
    ->attributes(
        type_attr('street', 'text'),
        type_attr('city', 'text'),
        type_attr('zip', 'text')
    );

echo $query->toSQL();
// CREATE TYPE address AS (street text, city text, zip text)
```

### With Schema

```php
<?php

use function Flow\PgQuery\DSL\{create_composite_type, type_attr};

$query = create_composite_type('public.address')
    ->attributes(
        type_attr('street', 'text')
    );

echo $query->toSQL();
// CREATE TYPE public.address AS (street text)
```

### With Collation

```php
<?php

use function Flow\PgQuery\DSL\{create_composite_type, type_attr};

$query = create_composite_type('person')
    ->attributes(
        type_attr('name', 'text')->collate('en_US')
    );

echo $query->toSQL();
// CREATE TYPE person AS (name text COLLATE "en_US")
```

## CREATE TYPE (Enum)

### Basic Enum Type

```php
<?php

use function Flow\PgQuery\DSL\create_enum_type;

$query = create_enum_type('status')
    ->labels('pending', 'active', 'closed');

echo $query->toSQL();
// CREATE TYPE status AS ENUM ('pending', 'active', 'closed')
```

### With Schema

```php
<?php

use function Flow\PgQuery\DSL\create_enum_type;

$query = create_enum_type('public.status')
    ->labels('pending', 'active');

echo $query->toSQL();
// CREATE TYPE public.status AS ENUM ('pending', 'active')
```

## CREATE TYPE (Range)

### Basic Range Type

```php
<?php

use function Flow\PgQuery\DSL\create_range_type;

$query = create_range_type('floatrange')
    ->subtype('float8');

echo $query->toSQL();
// CREATE TYPE floatrange AS RANGE (subtype = float8)
```

### With Schema

```php
<?php

use function Flow\PgQuery\DSL\create_range_type;

$query = create_range_type('public.floatrange')
    ->subtype('float8');

echo $query->toSQL();
// CREATE TYPE public.floatrange AS RANGE (subtype = float8)
```

### With Subtype Operator Class

```php
<?php

use function Flow\PgQuery\DSL\create_range_type;

$query = create_range_type('floatrange')
    ->subtype('float8')
    ->subtypeOpclass('float8_ops');

echo $query->toSQL();
// CREATE TYPE floatrange AS RANGE (subtype = float8, subtype_opclass = 'float8_ops')
```

### With Collation

```php
<?php

use function Flow\PgQuery\DSL\create_range_type;

$query = create_range_type('textrange')
    ->subtype('text')
    ->collation('en_US');

echo $query->toSQL();
// CREATE TYPE textrange AS RANGE (subtype = text, "collation" = 'en_US')
```

### With Canonical Function

```php
<?php

use function Flow\PgQuery\DSL\create_range_type;

$query = create_range_type('daterange')
    ->subtype('date')
    ->canonical('daterange_canonical');

echo $query->toSQL();
// CREATE TYPE daterange AS RANGE (subtype = date, canonical = 'daterange_canonical')
```

### With Subtype Diff Function

```php
<?php

use function Flow\PgQuery\DSL\create_range_type;

$query = create_range_type('floatrange')
    ->subtype('float8')
    ->subtypeDiff('float8mi');

echo $query->toSQL();
// CREATE TYPE floatrange AS RANGE (subtype = float8, subtype_diff = 'float8mi')
```

## ALTER TYPE (Enum)

### Add Value

```php
<?php

use function Flow\PgQuery\DSL\alter_enum_type;

$query = alter_enum_type('status')
    ->addValue('archived');

echo $query->toSQL();
// ALTER TYPE status ADD VALUE 'archived'
```

### Add Value IF NOT EXISTS

```php
<?php

use function Flow\PgQuery\DSL\alter_enum_type;

$query = alter_enum_type('status')
    ->addValue('archived')
    ->ifNotExists();

echo $query->toSQL();
// ALTER TYPE status ADD VALUE IF NOT EXISTS 'archived'
```

### Add Value BEFORE

```php
<?php

use function Flow\PgQuery\DSL\alter_enum_type;

$query = alter_enum_type('status')
    ->addValueBefore('pending', 'active');

echo $query->toSQL();
// ALTER TYPE status ADD VALUE 'pending' BEFORE 'active'
```

### Add Value AFTER

```php
<?php

use function Flow\PgQuery\DSL\alter_enum_type;

$query = alter_enum_type('status')
    ->addValueAfter('archived', 'closed');

echo $query->toSQL();
// ALTER TYPE status ADD VALUE 'archived' AFTER 'closed'
```

### Rename Value

```php
<?php

use function Flow\PgQuery\DSL\alter_enum_type;

$query = alter_enum_type('status')
    ->renameValue('old_name', 'new_name');

echo $query->toSQL();
// ALTER TYPE status RENAME VALUE 'old_name' TO 'new_name'
```

## DROP TYPE

### Simple Drop

```php
<?php

use function Flow\PgQuery\DSL\drop_type;

$query = drop_type('address');

echo $query->toSQL();
// DROP TYPE address
```

### IF EXISTS

```php
<?php

use function Flow\PgQuery\DSL\drop_type;

$query = drop_type('address')
    ->ifExists();

echo $query->toSQL();
// DROP TYPE IF EXISTS address
```

### CASCADE

Drop all objects that depend on the type:

```php
<?php

use function Flow\PgQuery\DSL\drop_type;

$query = drop_type('address')
    ->cascade();

echo $query->toSQL();
// DROP TYPE address CASCADE
```

### RESTRICT

Refuse to drop the type if any objects depend on it (default behavior):

```php
<?php

use function Flow\PgQuery\DSL\drop_type;

$query = drop_type('address')
    ->restrict();

echo $query->toSQL();
// DROP TYPE address
```

### Multiple Types

```php
<?php

use function Flow\PgQuery\DSL\drop_type;

$query = drop_type('address', 'status', 'floatrange');

echo $query->toSQL();
// DROP TYPE address, status, floatrange
```

### Combined Options

```php
<?php

use function Flow\PgQuery\DSL\drop_type;

$query = drop_type('address')
    ->ifExists()
    ->cascade();

echo $query->toSQL();
// DROP TYPE IF EXISTS address CASCADE
```

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/pg-query/namespaces/flow-pgquery-dsl.html).
