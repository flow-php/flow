# Function and Procedure Query Builder

- [Back](/documentation/components/libs/postgresql.md)

[TOC]

The Function and Procedure Query Builder provides a fluent, type-safe interface for constructing PostgreSQL function and procedure management statements:
CREATE FUNCTION, CREATE PROCEDURE, ALTER FUNCTION, ALTER PROCEDURE, DROP FUNCTION, DROP PROCEDURE, CALL, and DO.

## CREATE FUNCTION

### Basic Function Creation

```php
<?php

use function Flow\PostgreSql\DSL\{create, func_arg, column_type_integer};

$query = create()->function('add_numbers')
    ->arguments(func_arg(column_type_integer())->named('a'), func_arg(column_type_integer())->named('b'))
    ->returns('integer')
    ->language('sql')
    ->as('SELECT a + b');

echo $query->toSql();
// CREATE FUNCTION add_numbers(IN a "integer", IN b "integer") RETURNS "integer" LANGUAGE sql AS $$SELECT a + b$$
```

### PL/pgSQL Function

```php
<?php

use function Flow\PostgreSql\DSL\{create, func_arg, column_type_integer};

$query = create()->function('increment')
    ->arguments(func_arg(column_type_integer())->named('val'))
    ->returns('integer')
    ->language('plpgsql')
    ->as('BEGIN RETURN val + 1; END;');

echo $query->toSql();
// CREATE FUNCTION increment(IN val "integer") RETURNS "integer" LANGUAGE plpgsql AS $$BEGIN RETURN val + 1; END;$$
```

### OR REPLACE

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->function('my_func')
    ->orReplace()
    ->returns('integer')
    ->language('sql')
    ->as('SELECT 1');

echo $query->toSql();
// CREATE OR REPLACE FUNCTION my_func() RETURNS "integer" LANGUAGE sql AS $$SELECT 1$$
```

### RETURNS TABLE

Create a function that returns a table:

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->function('get_users')
    ->returnsTable(['id' => 'integer', 'name' => 'text'])
    ->language('sql')
    ->as('SELECT id, name FROM users');

echo $query->toSql();
// CREATE FUNCTION get_users() RETURNS TABLE (id "integer", name text) LANGUAGE sql AS $$SELECT id, name FROM users$$
```

### RETURNS SETOF

Create a function that returns a set of values:

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->function('get_user_ids')
    ->returnsSetOf('integer')
    ->language('sql')
    ->as('SELECT id FROM users');

echo $query->toSql();
// CREATE FUNCTION get_user_ids() RETURNS SETOF "integer" LANGUAGE sql AS $$SELECT id FROM users$$
```

### RETURNS VOID

Create a function that returns nothing:

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->function('log_event')
    ->returnsVoid()
    ->language('sql')
    ->as('INSERT INTO logs (msg) VALUES (current_timestamp)');

echo $query->toSql();
// CREATE FUNCTION log_event() RETURNS void LANGUAGE sql AS $$INSERT INTO logs (msg) VALUES (current_timestamp)$$
```

### Volatility (IMMUTABLE, STABLE, VOLATILE)

```php
<?php

use function Flow\PostgreSql\DSL\{create, func_arg, column_type_integer};

// IMMUTABLE - same input always produces same output
$query = create()->function('double')
    ->arguments(func_arg(column_type_integer())->named('x'))
    ->returns('integer')
    ->language('sql')
    ->immutable()
    ->as('SELECT x * 2');

// STABLE - returns same result within a single statement
$query = create()->function('get_config')
    ->returns('text')
    ->language('sql')
    ->stable()
    ->as('SELECT current_setting(\'app.name\')');

// VOLATILE (default) - can return different results on successive calls
$query = create()->function('get_time')
    ->returns('timestamp')
    ->language('sql')
    ->volatile()
    ->as('SELECT now()');
```

### PARALLEL Safety

```php
<?php

use function Flow\PostgreSql\DSL\{create, func_arg, column_type_integer};
use Flow\PostgreSql\QueryBuilder\Schema\Function\ParallelSafety;

$query = create()->function('compute')
    ->arguments(func_arg(column_type_integer())->named('x'))
    ->returns('integer')
    ->language('sql')
    ->parallel(ParallelSafety::SAFE)
    ->as('SELECT x * 2');

// Available options:
// ParallelSafety::UNSAFE - cannot be executed in parallel mode
// ParallelSafety::RESTRICTED - can be executed in parallel but only in group leader
// ParallelSafety::SAFE - can be executed in parallel in any process
```

### STRICT (RETURNS NULL ON NULL INPUT)

```php
<?php

use function Flow\PostgreSql\DSL\{create, func_arg, column_type_integer};

$query = create()->function('safe_add')
    ->arguments(func_arg(column_type_integer())->named('a'), func_arg(column_type_integer())->named('b'))
    ->returns('integer')
    ->language('sql')
    ->strict()  // Returns NULL if any input is NULL
    ->as('SELECT a + b');

// Or explicitly call on NULL input
$query = create()->function('nullable_add')
    ->arguments(func_arg(column_type_integer())->named('a'), func_arg(column_type_integer())->named('b'))
    ->returns('integer')
    ->language('sql')
    ->calledOnNullInput()  // Function is called even with NULL inputs
    ->as('SELECT COALESCE(a, 0) + COALESCE(b, 0)');
```

### SECURITY DEFINER / SECURITY INVOKER

```php
<?php

use function Flow\PostgreSql\DSL\create;

// Function runs with the privileges of the creator
$query = create()->function('admin_only')
    ->returns('void')
    ->language('sql')
    ->securityDefiner()
    ->as('SELECT 1');

// Function runs with the privileges of the caller (default)
$query = create()->function('normal_func')
    ->returns('void')
    ->language('sql')
    ->securityInvoker()
    ->as('SELECT 1');
```

### COST and ROWS

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->function('expensive_func')
    ->returns('integer')
    ->language('sql')
    ->cost(1000)  // Estimated execution cost
    ->as('SELECT 1');

$query = create()->function('many_rows')
    ->returnsSetOf('integer')
    ->language('sql')
    ->rows(10000)  // Estimated number of rows returned
    ->as('SELECT generate_series(1, 10000)');
```

### LEAKPROOF

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->function('no_side_effects')
    ->returns('boolean')
    ->language('sql')
    ->leakproof()  // Function has no side effects
    ->as('SELECT true');
```

### SET Configuration

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->function('my_func')
    ->returns('void')
    ->language('sql')
    ->set('search_path', 'public')
    ->as('SELECT 1');
```

### Function Arguments

The `func_arg()` DSL function creates function arguments with various options:

```php
<?php

use function Flow\PostgreSql\DSL\{func_arg, column_type_integer, column_type_text};

// Basic argument
$arg = func_arg(column_type_integer());

// Named argument
$arg = func_arg(column_type_integer())->named('user_id');

// With default value
$arg = func_arg(column_type_integer())->named('limit')->default('100');

// Argument modes
$arg = func_arg(column_type_integer())->in();       // IN - input only (default)
$arg = func_arg(column_type_integer())->out();      // OUT - output only
$arg = func_arg(column_type_integer())->inout();    // INOUT - both input and output
$arg = func_arg(column_type_text())->variadic();    // VARIADIC - variable number of arguments
```

### Complete Example

```php
<?php

use function Flow\PostgreSql\DSL\{create, func_arg, column_type_numeric};
use Flow\PostgreSql\QueryBuilder\Schema\Function\ParallelSafety;

$query = create()->function('calculate_discount')
    ->orReplace()
    ->arguments(
        func_arg(column_type_numeric())->named('price'),
        func_arg(column_type_numeric())->named('discount_percent')->default('10')
    )
    ->returns('numeric')
    ->language('sql')
    ->immutable()
    ->parallel(ParallelSafety::SAFE)
    ->strict()
    ->as('SELECT price - (price * discount_percent / 100)');
```

## CREATE PROCEDURE

### Basic Procedure Creation

```php
<?php

use function Flow\PostgreSql\DSL\{create, func_arg, column_type_integer};

$query = create()->procedure('update_stats')
    ->arguments(func_arg(column_type_integer())->named('user_id'))
    ->language('plpgsql')
    ->as('BEGIN UPDATE stats SET count = count + 1 WHERE id = user_id; END;');

echo $query->toSql();
// CREATE PROCEDURE update_stats(IN user_id "integer") LANGUAGE plpgsql AS $$BEGIN UPDATE stats SET count = count + 1 WHERE id = user_id; END;$$
```

### OR REPLACE

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->procedure('my_proc')
    ->orReplace()
    ->language('sql')
    ->as('SELECT 1');

echo $query->toSql();
// CREATE OR REPLACE PROCEDURE my_proc() LANGUAGE sql AS $$SELECT 1$$
```

### SECURITY DEFINER / SECURITY INVOKER

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->procedure('admin_proc')
    ->language('sql')
    ->securityDefiner()
    ->as('DELETE FROM temp_data');
```

### SET Configuration

```php
<?php

use function Flow\PostgreSql\DSL\create;

$query = create()->procedure('my_proc')
    ->language('sql')
    ->set('work_mem', '1GB')
    ->as('SELECT 1');
```

## ALTER FUNCTION

### Change Volatility

```php
<?php

use function Flow\PostgreSql\DSL\{alter, func_arg, column_type_integer};

$query = alter()->function('my_func')
    ->arguments(func_arg(column_type_integer()))
    ->immutable();

echo $query->toSql();
// ALTER FUNCTION my_func("integer") IMMUTABLE
```

### Change Parallel Safety

```php
<?php

use function Flow\PostgreSql\DSL\{alter, func_arg, column_type_integer};
use Flow\PostgreSql\QueryBuilder\Schema\Function\ParallelSafety;

$query = alter()->function('my_func')
    ->arguments(func_arg(column_type_integer()))
    ->parallel(ParallelSafety::SAFE);

echo $query->toSql();
// ALTER FUNCTION my_func("integer") PARALLEL safe
```

### RENAME TO

```php
<?php

use function Flow\PostgreSql\DSL\{alter, func_arg, column_type_text};

$query = alter()->function('old_name')
    ->arguments(func_arg(column_type_text()))
    ->renameTo('new_name');

echo $query->toSql();
// ALTER FUNCTION old_name(text) RENAME TO new_name
```

### Change COST and ROWS

```php
<?php

use function Flow\PostgreSql\DSL\{alter, func_arg, column_type_integer};

$query = alter()->function('my_func')
    ->arguments(func_arg(column_type_integer()))
    ->cost(500);

$query = alter()->function('set_returning_func')
    ->rows(1000);
```

### SET and RESET Configuration

```php
<?php

use function Flow\PostgreSql\DSL\alter;

// Set a configuration parameter
$query = alter()->function('my_func')
    ->set('search_path', 'public');

// Reset a configuration parameter
$query = alter()->function('my_func')
    ->reset('search_path');

// Reset all configuration parameters
$query = alter()->function('my_func')
    ->resetAll();
```

## ALTER PROCEDURE

### RENAME TO

```php
<?php

use function Flow\PostgreSql\DSL\{alter, func_arg, column_type_integer};

$query = alter()->procedure('old_proc')
    ->arguments(func_arg(column_type_integer()))
    ->renameTo('new_proc');

echo $query->toSql();
// ALTER PROCEDURE old_proc("integer") RENAME TO new_proc
```

### SECURITY DEFINER / SECURITY INVOKER

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->procedure('my_proc')
    ->securityDefiner();

echo $query->toSql();
// ALTER PROCEDURE my_proc SECURITY DEFINER
```

### SET and RESET Configuration

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->procedure('my_proc')
    ->set('work_mem', '2GB');

$query = alter()->procedure('my_proc')
    ->reset('work_mem');

$query = alter()->procedure('my_proc')
    ->resetAll();
```

## DROP FUNCTION

### Simple Drop

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->function('my_func');

echo $query->toSql();
// DROP FUNCTION my_func
```

### With Argument Types

```php
<?php

use function Flow\PostgreSql\DSL\{drop, func_arg, column_type_integer, column_type_text};

$query = drop()->function('my_func')
    ->arguments(func_arg(column_type_integer()), func_arg(column_type_text()));

echo $query->toSql();
// DROP FUNCTION my_func("integer", text)
```

### IF EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\{drop, func_arg, column_type_integer};

$query = drop()->function('my_func')
    ->ifExists()
    ->arguments(func_arg(column_type_integer()));

echo $query->toSql();
// DROP FUNCTION IF EXISTS my_func("integer")
```

### CASCADE

```php
<?php

use function Flow\PostgreSql\DSL\{drop, func_arg, column_type_integer, column_type_text};

$query = drop()->function('my_func')
    ->ifExists()
    ->arguments(func_arg(column_type_integer()), func_arg(column_type_text()))
    ->cascade();

echo $query->toSql();
// DROP FUNCTION IF EXISTS my_func("integer", text) CASCADE
```

### RESTRICT

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->function('my_func')
    ->restrict();

echo $query->toSql();
// DROP FUNCTION my_func RESTRICT
```

## DROP PROCEDURE

### Simple Drop

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->procedure('my_proc');

echo $query->toSql();
// DROP PROCEDURE my_proc
```

### With Options

```php
<?php

use function Flow\PostgreSql\DSL\{drop, func_arg, column_type_integer};

$query = drop()->procedure('my_proc')
    ->ifExists()
    ->arguments(func_arg(column_type_integer()))
    ->cascade();

echo $query->toSql();
// DROP PROCEDURE IF EXISTS my_proc("integer") CASCADE
```

## CALL (Procedure Invocation)

### Simple Call

```php
<?php

use function Flow\PostgreSql\DSL\call;

$query = call('update_stats');

echo $query->toSql();
// CALL update_stats()
```

### With Arguments

```php
<?php

use function Flow\PostgreSql\DSL\call;

$query = call('update_stats')
    ->with(123, 'test');

echo $query->toSql();
// CALL update_stats(123, 'test')
```

Arguments can be integers, strings, floats, booleans, or null:

```php
<?php

use function Flow\PostgreSql\DSL\call;

$query = call('process_data')
    ->with(42, 'hello', 3.14, true, null);

echo $query->toSql();
// CALL process_data(42, 'hello', 3.14, true, NULL)
```

## DO (Anonymous Code Block)

### Basic DO Block

```php
<?php

use function Flow\PostgreSql\DSL\do_block;

$query = do_block('BEGIN RAISE NOTICE $$Hello$$; END;');

echo $query->toSql();
// DO $outer$BEGIN RAISE NOTICE $$Hello$$; END;$outer$ LANGUAGE plpgsql
```

### With Language

```php
<?php

use function Flow\PostgreSql\DSL\do_block;

$query = do_block('SELECT 1')->language('sql');

echo $query->toSql();
// DO $$SELECT 1$$ LANGUAGE sql
```

## DSL Functions Reference

| Function | Description |
|----------|-------------|
| `func_arg(ColumnType $type)` | Create a function argument |
| `create()->function(string $name)` | Create a new function |
| `create()->procedure(string $name)` | Create a new procedure |
| `alter()->function(string $name)` | Alter an existing function |
| `alter()->procedure(string $name)` | Alter an existing procedure |
| `drop()->function(string $name)` | Drop a function |
| `drop()->procedure(string $name)` | Drop a procedure |
| `call(string $procedure)` | Call a procedure |
| `do_block(string $code)` | Execute an anonymous code block |

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/postgresql/namespaces/flow-postgresql-dsl.html).
