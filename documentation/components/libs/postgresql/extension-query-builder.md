# Extension Query Builder

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The Extension Query Builder provides a fluent, type-safe interface for constructing PostgreSQL extension management statements:
CREATE EXTENSION, ALTER EXTENSION, and DROP EXTENSION.

## CREATE EXTENSION

### Basic Extension Creation

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->extension('uuid-ossp');

echo $query->toSql();
// CREATE EXTENSION "uuid-ossp"
```

### IF NOT EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->extension('uuid-ossp')
    ->ifNotExists();

echo $query->toSql();
// CREATE EXTENSION IF NOT EXISTS "uuid-ossp"
```

### With Schema

Install the extension's objects in a specific schema:

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->extension('uuid-ossp')
    ->schema('public');

echo $query->toSql();
// CREATE EXTENSION "uuid-ossp" SCHEMA public
```

### With Version

Install a specific version of the extension:

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->extension('postgis')
    ->version('3.0');

echo $query->toSql();
// CREATE EXTENSION postgis VERSION "3.0"
```

### CASCADE

Automatically install any extensions that this extension depends on:

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->extension('postgis_raster')
    ->cascade();

echo $query->toSql();
// CREATE EXTENSION postgis_raster CASCADE
```

### Complete Example

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->extension('postgis')
    ->ifNotExists()
    ->schema('extensions')
    ->version('3.0')
    ->cascade();

echo $query->toSql();
// CREATE EXTENSION IF NOT EXISTS postgis SCHEMA extensions VERSION "3.0" CASCADE
```

## ALTER EXTENSION

### Update Extension

Update to the latest version:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->extension('postgis')
    ->update();

echo $query->toSql();
// ALTER EXTENSION postgis UPDATE
```

### Update to Specific Version

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->extension('postgis')
    ->updateTo('3.0');

echo $query->toSql();
// ALTER EXTENSION postgis UPDATE TO "3.0"
```

### Add Objects to Extension

Add a function to an extension:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->extension('my_extension')
    ->addFunction('my_func', ['integer', 'text']);

echo $query->toSql();
// ALTER EXTENSION my_extension ADD FUNCTION my_func("integer", text)
```

Add a table to an extension:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->extension('my_extension')
    ->addTable('my_table');

echo $query->toSql();
// ALTER EXTENSION my_extension ADD TABLE my_table
```

### Drop Objects from Extension

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->extension('my_extension')
    ->dropFunction('my_func', ['integer']);

echo $query->toSql();
// ALTER EXTENSION my_extension DROP FUNCTION my_func("integer")

$query = alter()->extension('my_extension')
    ->dropTable('my_table');

echo $query->toSql();
// ALTER EXTENSION my_extension DROP TABLE my_table
```

## DROP EXTENSION

### Simple Drop

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->extension('uuid-ossp');

echo $query->toSql();
// DROP EXTENSION "uuid-ossp"
```

### IF EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->extension('uuid-ossp')
    ->ifExists();

echo $query->toSql();
// DROP EXTENSION IF EXISTS "uuid-ossp"
```

### CASCADE

Drop all objects that depend on the extension:

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->extension('postgis')
    ->cascade();

echo $query->toSql();
// DROP EXTENSION postgis CASCADE
```

### RESTRICT

Refuse to drop the extension if any objects depend on it (default behavior):

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->extension('uuid-ossp')
    ->restrict();

echo $query->toSql();
// DROP EXTENSION "uuid-ossp"
```

### Multiple Extensions

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->extension('postgis', 'postgis_raster', 'postgis_topology');

echo $query->toSql();
// DROP EXTENSION postgis, postgis_raster, postgis_topology
```

### Combined Options

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->extension('postgis')
    ->ifExists()
    ->cascade();

echo $query->toSql();
// DROP EXTENSION IF EXISTS postgis CASCADE
```

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/postgresql/namespaces/flow-postgresql-dsl.html).
