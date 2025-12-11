# Schema Query Builder

- [⬅️ Back](/documentation/components/libs/pg-query.md)

[TOC]

The Schema Query Builder provides a fluent interface for building PostgreSQL schema management statements.

## CREATE SCHEMA

Create a new schema in the database.

```php
<?php

use function Flow\PgQuery\DSL\create;

// Simple schema creation
$query = create()->schema('my_schema');

echo $query->toSQL();
// CREATE SCHEMA my_schema

// With IF NOT EXISTS
$query = create()->schema('my_schema')
    ->ifNotExists();

echo $query->toSQL();
// CREATE SCHEMA IF NOT EXISTS my_schema

// With authorization
$query = create()->schema('my_schema')
    ->authorization('admin_user');

echo $query->toSQL();
// CREATE SCHEMA my_schema AUTHORIZATION admin_user

// Combined options
$query = create()->schema('my_schema')
    ->ifNotExists()
    ->authorization('admin_user');

echo $query->toSQL();
// CREATE SCHEMA IF NOT EXISTS my_schema AUTHORIZATION admin_user
```

### Available Methods

| Method | Description |
|--------|-------------|
| `ifNotExists()` | Add IF NOT EXISTS clause |
| `authorization(string $role)` | Specify schema owner |

## ALTER SCHEMA

Modify an existing schema.

### Rename Schema

```php
<?php

use function Flow\PgQuery\DSL\alter;

$query = alter()->schema('old_schema')
    ->renameTo('new_schema');

echo $query->toSQL();
// ALTER SCHEMA old_schema RENAME TO new_schema
```

### Change Owner

```php
<?php

use function Flow\PgQuery\DSL\alter;

$query = alter()->schema('my_schema')
    ->ownerTo('new_owner');

echo $query->toSQL();
// ALTER SCHEMA my_schema OWNER TO new_owner
```

## DROP SCHEMA

Remove one or more schemas from the database.

```php
<?php

use function Flow\PgQuery\DSL\drop;

// Simple drop
$query = drop()->schema('my_schema');

echo $query->toSQL();
// DROP SCHEMA my_schema

// With IF EXISTS
$query = drop()->schema('my_schema')
    ->ifExists();

echo $query->toSQL();
// DROP SCHEMA IF EXISTS my_schema

// With CASCADE
$query = drop()->schema('my_schema')
    ->cascade();

echo $query->toSQL();
// DROP SCHEMA my_schema CASCADE

// Combined options
$query = drop()->schema('my_schema')
    ->ifExists()
    ->cascade();

echo $query->toSQL();
// DROP SCHEMA IF EXISTS my_schema CASCADE

// Drop multiple schemas
$query = drop()->schema('schema1', 'schema2', 'schema3')
    ->cascade();

echo $query->toSQL();
// DROP SCHEMA schema1, schema2, schema3 CASCADE
```

### Available Methods

| Method | Description |
|--------|-------------|
| `ifExists()` | Add IF EXISTS clause |
| `cascade()` | Drop dependent objects |
| `restrict()` | Refuse to drop if dependent objects exist (default) |

## DSL Functions

| Function | Returns | Description |
|----------|---------|-------------|
| `create()->schema(string $name)` | `CreateSchemaOptionsStep` | Start building a CREATE SCHEMA statement |
| `alter()->schema(string $name)` | `AlterSchemaActionStep` | Start building an ALTER SCHEMA statement |
| `drop()->schema(string ...$names)` | `DropSchemaFinalStep` | Start building a DROP SCHEMA statement |

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/pg-query/namespaces/flow-pgquery-dsl.html).
