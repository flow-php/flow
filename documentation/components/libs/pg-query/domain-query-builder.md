# Domain Query Builder

- [Back](/documentation/components/libs/pg-query.md)

[TOC]

The Domain Query Builder provides a fluent, type-safe interface for constructing PostgreSQL domain management statements:
CREATE DOMAIN, ALTER DOMAIN, and DROP DOMAIN.

Domains are user-defined data types with optional constraints. They are useful for defining reusable data types with specific validation rules.

## CREATE DOMAIN

### Basic Domain Creation

```php
<?php

use function Flow\PgQuery\DSL\create_domain;

$query = create_domain('email')
    ->as('text');

echo $query->toSQL();
// CREATE DOMAIN email AS text
```

### With Schema

```php
<?php

use function Flow\PgQuery\DSL\create_domain;

$query = create_domain('public.email')
    ->as('text');

echo $query->toSQL();
// CREATE DOMAIN public.email AS text
```

### NOT NULL

```php
<?php

use function Flow\PgQuery\DSL\create_domain;

$query = create_domain('email')
    ->as('text')
    ->notNull();

echo $query->toSQL();
// CREATE DOMAIN email AS text NOT NULL
```

### NULL

Explicitly allow NULL values:

```php
<?php

use function Flow\PgQuery\DSL\create_domain;

$query = create_domain('email')
    ->as('text')
    ->null();

echo $query->toSQL();
// CREATE DOMAIN email AS text NULL
```

### DEFAULT

Set a default value:

```php
<?php

use function Flow\PgQuery\DSL\create_domain;

$query = create_domain('email')
    ->as('text')
    ->default("'default@example.com'");

echo $query->toSQL();
// CREATE DOMAIN email AS text DEFAULT 'default@example.com'
```

### CHECK Constraint

Add a CHECK constraint:

```php
<?php

use function Flow\PgQuery\DSL\create_domain;

$query = create_domain('positive_int')
    ->as('int4')
    ->check('VALUE > 0');

echo $query->toSQL();
// CREATE DOMAIN positive_int AS int4 CHECK (value > 0)
```

### Named Constraint

Add a named CHECK constraint:

```php
<?php

use function Flow\PgQuery\DSL\create_domain;

$query = create_domain('positive_int')
    ->as('int4')
    ->constraint('positive_check')
    ->check('VALUE > 0');

echo $query->toSQL();
// CREATE DOMAIN positive_int AS int4 CONSTRAINT positive_check CHECK (value > 0)
```

### COLLATE

Specify a collation:

```php
<?php

use function Flow\PgQuery\DSL\create_domain;

$query = create_domain('email')
    ->as('text')
    ->collate('en_US');

echo $query->toSQL();
// CREATE DOMAIN email AS text COLLATE "en_US"
```

### Multiple Constraints

```php
<?php

use function Flow\PgQuery\DSL\create_domain;

$query = create_domain('email')
    ->as('text')
    ->notNull()
    ->check("VALUE ~ '^.+@.+$'");

echo $query->toSQL();
// CREATE DOMAIN email AS text NOT NULL CHECK (value ~ '^.+@.+$')
```

## ALTER DOMAIN

### SET NOT NULL

```php
<?php

use function Flow\PgQuery\DSL\alter_domain;

$query = alter_domain('email')
    ->setNotNull();

echo $query->toSQL();
// ALTER DOMAIN email SET NOT NULL
```

### DROP NOT NULL

```php
<?php

use function Flow\PgQuery\DSL\alter_domain;

$query = alter_domain('email')
    ->dropNotNull();

echo $query->toSQL();
// ALTER DOMAIN email DROP NOT NULL
```

### SET DEFAULT

```php
<?php

use function Flow\PgQuery\DSL\alter_domain;

$query = alter_domain('email')
    ->setDefault("'default@example.com'");

echo $query->toSQL();
// ALTER DOMAIN email SET DEFAULT 'default@example.com'
```

### DROP DEFAULT

```php
<?php

use function Flow\PgQuery\DSL\alter_domain;

$query = alter_domain('email')
    ->dropDefault();

echo $query->toSQL();
// ALTER DOMAIN email DROP DEFAULT
```

### ADD CONSTRAINT

```php
<?php

use function Flow\PgQuery\DSL\alter_domain;

$query = alter_domain('email')
    ->addConstraint('valid_email', "VALUE ~ '^.+@.+$'");

echo $query->toSQL();
// ALTER DOMAIN email ADD CONSTRAINT valid_email CHECK (value ~ '^.+@.+$')
```

### DROP CONSTRAINT

```php
<?php

use function Flow\PgQuery\DSL\alter_domain;

$query = alter_domain('email')
    ->dropConstraint('valid_email');

echo $query->toSQL();
// ALTER DOMAIN email DROP CONSTRAINT valid_email
```

### DROP CONSTRAINT CASCADE

```php
<?php

use function Flow\PgQuery\DSL\alter_domain;

$query = alter_domain('email')
    ->dropConstraint('valid_email')
    ->cascade();

echo $query->toSQL();
// ALTER DOMAIN email DROP CONSTRAINT valid_email CASCADE
```

### VALIDATE CONSTRAINT

Validate an existing constraint:

```php
<?php

use function Flow\PgQuery\DSL\alter_domain;

$query = alter_domain('email')
    ->validateConstraint('valid_email');

echo $query->toSQL();
// ALTER DOMAIN email VALIDATE CONSTRAINT valid_email
```

### With Schema

```php
<?php

use function Flow\PgQuery\DSL\alter_domain;

$query = alter_domain('public.email')
    ->setNotNull();

echo $query->toSQL();
// ALTER DOMAIN public.email SET NOT NULL
```

## DROP DOMAIN

### Simple Drop

```php
<?php

use function Flow\PgQuery\DSL\drop_domain;

$query = drop_domain('email');

echo $query->toSQL();
// DROP DOMAIN email
```

### IF EXISTS

```php
<?php

use function Flow\PgQuery\DSL\drop_domain;

$query = drop_domain('email')
    ->ifExists();

echo $query->toSQL();
// DROP DOMAIN IF EXISTS email
```

### CASCADE

Drop all objects that depend on the domain:

```php
<?php

use function Flow\PgQuery\DSL\drop_domain;

$query = drop_domain('email')
    ->cascade();

echo $query->toSQL();
// DROP DOMAIN email CASCADE
```

### RESTRICT

Refuse to drop the domain if any objects depend on it (default behavior):

```php
<?php

use function Flow\PgQuery\DSL\drop_domain;

$query = drop_domain('email')
    ->restrict();

echo $query->toSQL();
// DROP DOMAIN email
```

### Multiple Domains

```php
<?php

use function Flow\PgQuery\DSL\drop_domain;

$query = drop_domain('email', 'phone', 'url');

echo $query->toSQL();
// DROP DOMAIN email, phone, url
```

### Combined Options

```php
<?php

use function Flow\PgQuery\DSL\drop_domain;

$query = drop_domain('email')
    ->ifExists()
    ->cascade();

echo $query->toSQL();
// DROP DOMAIN IF EXISTS email CASCADE
```

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/pg-query/namespaces/flow-pgquery-dsl.html).
