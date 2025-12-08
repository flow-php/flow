# Schema Query Builder

The Schema Query Builder provides a fluent interface for building PostgreSQL schema management statements.

## CREATE SCHEMA

Create a new schema in the database.

```php
use function Flow\PgQuery\DSL\create_schema;

// Simple schema creation
$sql = create_schema('my_schema')
    ->toAst();
// CREATE SCHEMA my_schema

// With IF NOT EXISTS
$sql = create_schema('my_schema')
    ->ifNotExists()
    ->toAst();
// CREATE SCHEMA IF NOT EXISTS my_schema

// With authorization
$sql = create_schema('my_schema')
    ->authorization('admin_user')
    ->toAst();
// CREATE SCHEMA my_schema AUTHORIZATION admin_user

// Combined options
$sql = create_schema('my_schema')
    ->ifNotExists()
    ->authorization('admin_user')
    ->toAst();
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
use function Flow\PgQuery\DSL\alter_schema;

$sql = alter_schema('old_schema')
    ->renameTo('new_schema')
    ->toAst();
// ALTER SCHEMA old_schema RENAME TO new_schema
```

### Change Owner

```php
use function Flow\PgQuery\DSL\alter_schema;

$sql = alter_schema('my_schema')
    ->ownerTo('new_owner')
    ->toAst();
// ALTER SCHEMA my_schema OWNER TO new_owner
```

## DROP SCHEMA

Remove one or more schemas from the database.

```php
use function Flow\PgQuery\DSL\drop_schema;

// Simple drop
$sql = drop_schema('my_schema')
    ->toAst();
// DROP SCHEMA my_schema

// With IF EXISTS
$sql = drop_schema('my_schema')
    ->ifExists()
    ->toAst();
// DROP SCHEMA IF EXISTS my_schema

// With CASCADE
$sql = drop_schema('my_schema')
    ->cascade()
    ->toAst();
// DROP SCHEMA my_schema CASCADE

// Combined options
$sql = drop_schema('my_schema')
    ->ifExists()
    ->cascade()
    ->toAst();
// DROP SCHEMA IF EXISTS my_schema CASCADE

// Drop multiple schemas
$sql = drop_schema('schema1', 'schema2', 'schema3')
    ->cascade()
    ->toAst();
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
| `create_schema(string $name)` | `CreateSchemaOptionsStep` | Start building a CREATE SCHEMA statement |
| `alter_schema(string $name)` | `AlterSchemaActionStep` | Start building an ALTER SCHEMA statement |
| `drop_schema(string ...$names)` | `DropSchemaFinalStep` | Start building a DROP SCHEMA statement |
