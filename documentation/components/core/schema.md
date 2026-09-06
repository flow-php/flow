# Schema

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Schema defines the structure and validation rules for DataFrame data. It provides type safety, data validation, and
metadata management for your data processing pipelines.

## Understanding Schema Components

A schema consists of column definitions that specify:

- **Name**: The column identifier
- **Type**: The expected data type (class string)
- **Nullable**: Whether NULL values are permitted
- **Metadata**: Key-value pairs for additional context

Structure field order is part of a structure type's identity: `structure{a, b}` and `structure{b, a}`
are different types, so `Schema::isSame()` and `Definition::isSame()` distinguish them. Value-level
comparisons (`Row::isEqual()`, `Rows::unique()`) stay field-order-insensitive.

## Arrays in a Schema

`type_array()` declares a `json` column. The data layer has no array type - parquet, Spark and Floe all express an
array as a JSON object or a JSON collection - so a declared `array<mixed>` is projected onto `json`, and the value is
stored as a `Flow\Types\Value\Json`, which preserves whether it was an object or a collection.

```php
definition_from_type('tags', type_array())->type()->toString();   // "json"
```

The projection applies at every depth, not only to whole columns. A structure field, list element or map value declared
as `array<mixed>` is rewritten to `json` when the definition is built:

```php
structure_schema('user', type_structure(['tags' => type_array()]))->type()->toString();   // "structure{tags: json}"
list_schema('batches', type_list(type_array()))->type()->toString();   // "list<json>"
map_schema('translations', type_map(type_string(), type_array()))->type()->toString();   // "map<string, json>"
```

The empty array `array{}` is **not** a column type - it is a value whose element type was never observed. Declaring one
is refused, because a column typed `array{}` could never hold anything:

```php
definition_from_type('tags', type_empty_array());
// RuntimeException: Column "tags" cannot be typed as array{} - an empty array is a value, not a
// column type. Declare the element type, e.g. type_list(type_string()).
```

Declare the concrete shape whenever it is known - `type_list()`, `type_map()` or `type_structure()` keep element typing
that `json` throws away, and adapters can map them onto native nested types.

## Schema Inference

Sources fall into two groups when no schema is declared.

**Sources that infer one.** CSV, Excel, JSON and JSON lines sample the file - and Google Sheet the sheet - before the
first row is read, decide one schema, and every batch they yield carries exactly that schema -
`from_csv(...)->schema()`, `from_excel(...)->schema()`, `from_json(...)->schema()`, `from_json_lines(...)->schema()`
and `from_google_sheet(...)->schema()` answer without running the pipeline. Tune the sample with
`->inferSchema(infer_schema()->sampleSize(...)->filesToSniff(...)->types(...)->allStrings()->unionByName())`, where
`filesToSniff()` counts files and so does nothing for Google Sheet, which reads a single sheet. `types()` restricts
which types inference may produce (`allStrings()` is sugar for `types(type_string())`), and `unionByName()` reads
sources with differing column sets as one wider schema, instead of rejecting them (CSV, Excel and Google Sheet,
which check the column set against the header) or dropping the columns a later source introduces (JSON, which has no
header to check). Every inferred column is nullable, and where narrowing is not safe the column floors to `string`.

**Sources that do not.** For the rest, every value still gets its type detected as rows are created and each batch
carries its own schema; `DataFrame::schema()` runs the pipeline and merges the per-batch schemas into one.
`printSchema()` prints either.

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array([
        ['id' => 1, 'tags' => ['new', 'sale']],
        ['id' => 2, 'tags' => []],
    ]))
    ->schema();

// id: ?integer
// tags: ?list<?string>
```

Inference sees only the values in front of it:

- `[]` detects as `list<null>` - a collection whose element type was never observed. It is the bottom of the type
  lattice, so it unifies with any other list rather than destroying its element type. In the example the first row
  infers `tags: list<string>`, and merging it with the second row's empty array yields `list<?string>`: the element
  is still a string, but it is no longer known to be present in every row.
- An array with mixed value types detects as `array<mixed>` - `json` in the schema, because no narrower type fits.
- Sources that transport everything as text (CSV) infer every column as `string`.

Inference is a fallback. When the source schema is known, declare it on the extractor ("Declaring the Source Schema"
below); when it is not, `autoCast()` can narrow the obvious cases ("Automatic Casting" below).

## When Two Types Disagree

A column holds exactly one type. When two rows disagree, the schema merge widens them to the narrowest type that can
hold both, and the result is fixed:

| both sides are | result |
|---|---|
| containers (`json`, `list`, `map`, `structure`) | `json` |
| anything else | `string` |

```php
data_frame()
    ->read(from_array([['a' => 1], ['a' => true]]))
    ->schema();

// a: ?string
```

**A union is not a column type.** `null|T` is the single exception, and it does not mean "either" - it means a nullable
column of `T`. Any other union is refused when the definition is built:

```php
definition_from_type('value', type_union(type_string(), type_null()));
// StringDefinition, nullable

definition_from_type('value', type_union(type_string(), type_integer()));
// UnsupportedUnionTypeException: Column "value" cannot be typed as "integer|string": a column holds
// exactly one type. Only "null|T" is a valid union - that is a nullable column.
// Possible fixes:
// * Declare the widest common type: str_schema('value')
// * Declare json_schema('value') when the shape is genuinely dynamic
```

## Declaring the Source Schema

Most extractors accept a schema up front: fluent `withSchema(Schema)`, or the optional `$schema` argument of the
`from_*` DSL function. Declared types drive row creation instead of inference, and the row keeps only the columns the
schema mentions.

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{bool_schema, data_frame, float_schema, int_schema, schema, str_schema};

data_frame()
    ->read(from_csv(__DIR__ . '/orders.csv')->withSchema(schema(
        int_schema('id'),
        str_schema('customer'),
        float_schema('total'),
        bool_schema('paid'),
    )))
    ->printSchema();

// schema
// |-- id: integer
// |-- customer: string
// |-- total: float
// |-- paid: boolean
```

The same works for in-memory data - declaring `tags` as a list keeps the element type the inference example above
threw away:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, list_schema, schema};
use function Flow\Types\DSL\{type_list, type_string};

data_frame()
    ->read(from_array([
        ['id' => 1, 'tags' => ['new', 'sale']],
        ['id' => 2, 'tags' => []],
    ])->withSchema(schema(
        int_schema('id'),
        list_schema('tags', type_list(type_string())),
    )))
    ->printSchema();

// schema
// |-- id: integer
// |-- tags: list<string>
```

The HTTP extractors accept a schema the same way - see
[Typing the row with a schema](/documentation/components/adapters/http.md) for declaring the response body as a
`structure`.

Self-descriptive formats carry their schema in the file itself, so their extractors read it from there and have no
`withSchema()` - Parquet and Floe in this repository.

### PostgreSQL sources describe themselves, and keep `withSchema()`

The `from_pgsql_cursor()`, `from_pgsql_limit_offset()` and `from_pgsql_key_set()` extractors derive their schema from
the database. Unlike a file format, a driver's type mapping is lossy, so these sources keep `withSchema()` as an
override that always wins:

```php
<?php

use function Flow\ETL\Adapter\PostgreSql\from_pgsql_cursor;
use function Flow\ETL\DSL\{data_frame, float_schema, int_schema, schema, to_output};

// Derived: one zero-row probe of your own query, memoised for the whole read.
data_frame()
    ->read(from_pgsql_cursor($client, 'SELECT id, amount FROM orders ORDER BY id'))
    ->write(to_output())
    ->run();

// Declared: withSchema() short-circuits the probe entirely and costs no query.
data_frame()
    ->read(
        from_pgsql_cursor($client, 'SELECT id, amount FROM orders ORDER BY id')
            ->withSchema(schema(int_schema('id'), float_schema('amount'))),
    )
    ->write(to_output())
    ->run();
```

How the derivation behaves:

- It **executes a zero-row probe** of your query (`SELECT * FROM (<your sql>) flow_describe LIMIT 0`) and reads the
  result's column metadata. It describes the *result*, never the catalog, so a view, an alias and a computed column
  all describe correctly.
- A **parameterised query describes normally**. The probe binds `null` at every parameter position, so it never sees
  the values you passed.
- **Every derived column is nullable.** Result metadata cannot prove that a column is `NOT NULL` - an outer join or an
  expression column can always produce `null`.
- The schema is derived **once per read**, before the first row, and every batch conforms to it.

A query that cannot be described **does not read at all**; it throws `SchemaNotDerivableException` with the reason.
That happens for exactly two things:

1. a query shape that cannot be wrapped in a zero-row `SELECT` - multi-statement, a data-modifying CTE, or
   `INSERT ... RETURNING`;
2. a column whose PostgreSQL type Flow has no type for - after type mapping that is only `record` and the geometric
   types (`point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle`).

In both cases `->withSchema(...)` is the escape hatch, and it skips the probe.

### Doctrine DBAL sources describe themselves, and keep `withSchema()`

`from_dbal_query()`, `from_dbal_queries()`, `from_dbal_limit_offset()`, `from_dbal_limit_offset_qb()` and
`from_dbal_key_set_qb()` derive their schema from a probe of the extractor's own SQL, memoised for the whole
read, describing the *result* and never the catalog - so a view, an alias and a computed column describe
correctly, and a query builder's paging is never part of what is described. `withSchema()` stays as an override
that always wins and costs no query.

How the probe runs depends on what the driver can answer:

- **MySQL** prepares your SQL as written and reads the statement's own metadata. Nothing is executed.
- **PostgreSQL** and **SQLite** run a zero-row query, `SELECT * FROM (<your sql>) flow_describe WHERE 1=0`.

```php
<?php

use function Flow\ETL\Adapter\Doctrine\from_dbal_query;
use function Flow\ETL\DSL\{data_frame, float_schema, int_schema, schema, to_output};

// Derived: the column names come through DBAL, the column types from the driver's own metadata.
data_frame()
    ->read(from_dbal_query($connection, 'SELECT id, amount * 2 AS total FROM orders WHERE id > :min', ['min' => 10]))
    ->write(to_output())
    ->run();

// Declared: withSchema() short-circuits the probe entirely.
data_frame()
    ->read(
        from_dbal_query($connection, 'SELECT id, amount * 2 AS total FROM orders')
            ->withSchema(schema(int_schema('id'), float_schema('total'))),
    )
    ->write(to_output())
    ->run();
```

What each driver answers:

- **PostgreSQL (`pgsql`)** and **MySQL (`mysqli`)** report real column types, mapped to Flow types; every derived
  column is nullable. MySQL types are read off a prepared statement, so its type probe executes nothing.
- **SQLite (`sqlite3`, `pdo_sqlite`)** has no per-column result types, so every column describes as nullable
  `string` and the read casts its values to match - the same model DuckDB uses for `sqlite_query()`.
- A **parameterised query describes normally**: DBAL's `:name` and `?` placeholders are rewritten to the driver's own
  dialect and the probe binds `null` at every position (an empty list for an array-typed parameter), so it never
  sees the values you passed.
- **Duplicate output column names** - `SELECT * FROM a JOIN b ON a.id = b.id` where both tables have `id` - collapse
  to the last one on PostgreSQL and MySQL, exactly as the row itself collapses them. On SQLite the zero-row probe
  makes the driver rename the second to `id:1`, so the schema carries a column the rows do not; alias the columns
  apart, or declare the schema, when a SQLite query selects a name twice.

A query that cannot be described **does not read at all**; it throws `SchemaNotDerivableException` with the reason.
That happens for exactly three things:

1. a query shape that cannot be wrapped in a zero-row `SELECT` - multi-statement, a data-modifying CTE, or
   `INSERT ... RETURNING`;
2. a driver this adapter has no result-type probe for - `pdo_pgsql` and `pdo_mysql` hand out a plain `PDO`, which
   has no arm yet, so they are refused by name; use the native `pgsql://` or `mysqli://` DSN, or `withSchema()`;
3. a column whose driver type Flow has no type for - on PostgreSQL that includes arrays, `money`, `inet`, ranges and
   `interval` on this route (the PostgreSQL adapter above maps more of them); on MySQL `BIT` and `GEOMETRY`.

In all three cases `->withSchema(...)` is the escape hatch, and it skips the probe.

## Automatic Casting

`DataFrame::autoCast()` detects every value in the pipeline: strings are narrowed to `null`, `boolean`, `integer`,
`float`, `datetime`, `date`, `uuid`, `json` or `xml` (and `html` on PHP 8.4+) when they parse as one, and arrays
mixing integers with floats are unified to floats. Use it when the source has no schema to declare and does not infer
one either - CSV does infer one, so it no longer needs `autoCast()` to be typed:

```php
<?php

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;

data_frame()
    ->read(from_array([['id' => '1', 'customer' => 'Norbert', 'total' => '10.5', 'paid' => 'true']]))
    ->autoCast()
    ->printSchema();

// schema
// |-- id: integer
// |-- customer: string
// |-- total: float
// |-- paid: boolean
```

`autoCast()` guesses per value, so a column can still come out mixed ("123" in one row is an integer, "abc" in the
next stays a string). A declared schema ("Declaring the Source Schema" above) is always the stronger option.

## Schema Validation Strategies

Flow PHP provides three built-in validation strategies:

- **[StrictValidator](/src/core/etl/src/Flow/ETL/Schema/Validator/StrictValidator.php)** - Rows must exactly match the
  schema; extra entries cause validation failure
- **[SelectiveValidator](/src/core/etl/src/Flow/ETL/Schema/Validator/SelectiveValidator.php)** - Only validates entries
  defined in schema; ignores extra entries
- **[EvolvingValidator](/src/core/etl/src/Flow/ETL/Schema/Validator/EvolvingValidator.php)** - Allows missing and extra
  entries as long as nullability permits it

By default, DataFrame uses `StrictValidator`, but you can specify a different validator as the second parameter to
`DataFrame::match()`.

## Basic Schema Matching

Use `DataFrame::match()` to validate data against a schema:

```php 
<?php

use function Flow\ETL\DSL\{data_frame, from_array, schema, int_schema, str_schema, bool_schema, to_output};
use Flow\ETL\Schema\Metadata;

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Product 1', 'active' => true],
        ['id' => 2, 'name' => 'Product 2', 'active' => false],
        ['id' => 3, 'name' => 'Product 3', 'active' => true]
    ]))
    ->match(
        schema(
            int_schema('id', $nullable = false),
            str_schema('name', $nullable = false),
            bool_schema('active', $nullable = false, Metadata::empty()->add('key', 'value')),
        )
    )
    ->write(to_output(false, Output::rows_and_schema))
    ->run();
```

## Schema Validation with Selective Strategy

```php
<?php

// Only validate defined fields, ignore extra ones
data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'John', 'extra_field' => 'ignored'],
        ['id' => 2, 'name' => 'Jane', 'another_extra' => 'also ignored'],
    ]))
    ->match(
        schema(
            int_schema('id'),
            str_schema('name')
        ),
        schema_selective_validator() // Only validate id and name, ignore other fields
    )
    ->write(to_output())
    ->run();
```