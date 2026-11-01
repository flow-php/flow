# Schema

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

A schema defines the structure of DataFrame data and the rules that the data must follow. It gives type safety, data
validation and metadata to your pipelines.

## Understanding Schema Components

A schema is a list of column definitions. Each definition has:

- Name: The identifier of the column
- Type: The expected data type (class string)
- Nullable: If the column accepts NULL values
- Metadata: Key-value pairs with more information about the column

The order of the fields is part of the identity of a structure type. `structure{a, b}` and `structure{b, a}`
are different types, so `Schema::isSame()` and `Definition::isSame()` tell them apart. The plan refuses a comparison of
two such columns (`ref('a')->equals(ref('b'))`). To compare them, first cast one of them to the type of the other.

## Arrays in a Schema

`type_array()` declares a `json` column. The data layer has no array type. Parquet and Floe both express an array
as a JSON object or a JSON collection. For this reason, a declared `array<mixed>` is projected onto `json`. The value
is stored as a `Flow\Types\Value\Json`. It remembers whether it was an object or a collection.

```php
definition_from_type('tags', type_array())->type()->toString();   // "json"
```

The projection applies at every depth, not only to whole columns. When the definition is built, a structure field,
list element or map value declared as `array<mixed>` becomes `json`:

```php
structure_schema('user', type_structure(['tags' => type_array()]))->type()->toString();   // "structure{tags: json}"
list_schema('batches', type_list(type_array()))->type()->toString();   // "list<json>"
map_schema('translations', type_map(type_string(), type_array()))->type()->toString();   // "map<string, json>"
```

The empty array `array{}` is not a column type. It is a value whose element type was never observed. A declaration
of one is refused, because a column typed `array{}` can never hold anything:

```php
definition_from_type('tags', type_empty_array());
// RuntimeException: Column "tags" cannot be typed as array{} - an empty array is a value, not a
// column type. Declare the element type, e.g. type_list(type_string()).
```

If the concrete shape is known, declare it. `type_list()`, `type_map()` or `type_structure()` keep the element types
that `json` throws away. Adapters can map them onto native nested types.

## Schema Inference

If no schema is declared, there are two groups of sources.

Sources that infer one. CSV, Excel, JSON and JSON lines sample the file before they read the first row. Google
Sheet samples the sheet. They decide one schema, and every batch that they yield has exactly that schema.
`from_csv(...)->schema()`, `from_excel(...)->schema()`, `from_json(...)->schema()`, `from_json_lines(...)->schema()`
and `from_google_sheet(...)->schema()` give the answer and do not run the pipeline. To tune the sample, use
`->inferSchema(infer_schema()->sampleSize(...)->filesToSniff(...)->types(...)->allStrings()->unionByName())`.
`filesToSniff()` counts only files that yielded a row, so a glob of empty files is opened in full. It does nothing
for Google Sheet, which reads a single sheet. `types()` limits the types that inference can produce.
`allStrings()` is a short form of `types(type_string())`. `unionByName()` reads sources with different column sets
as one wider schema. Without it, CSV, Excel and Google Sheet reject such sources, because they compare the column
set with the header. JSON has no header to compare, so it drops the columns that a later source adds. Every inferred
column is nullable. Where narrowing is not safe, the column falls back to `string`.

`from_array()`, `from_memory()` and the `from_sequence_*()` extractors infer the same way, from their first
`sampleSize` rows (20,480 by default). If `from_array()` gets an iterable that is not an array, it types every row.
If a row after the sample does not fit the inferred type, the read fails with an `InferredSchemaException`. The
exception names that row. To prevent this, infer from every row with `->inferSchema(infer_schema()->everyRow())`,
or declare the schema.

Sources that do not. The other sources know their schema before the first row. Parquet and Floe read it from the
file. PostgreSQL and DBAL derive it from the query (see below). The text, XML and HTTP extractors yield fixed columns.

`DataFrame::schema()` describes the plan, not the data. It starts from the `schema()` of the source and walks the
steps of the pipeline once. It gives the output schema of each step to the next step. For this reason, it can do the
I/O that the source needs to describe itself, for example a CSV sniff or a Parquet footer. It never reads a row. If
a step cannot name a column before rows flow, that is a build error. The error comes at that point, not in the
middle of the run. `select('nope')` and
`ref('a')->greaterThan(ref('b'))`
over incomparable types both refuse at the first trigger. `printSchema()` formats that same answer and runs
nothing. A plan that contains `joinEach()` cannot be described this way, so it refuses.

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

Inference sees only the values that it reads:

- `[]` detects as `list<null>`, that is, a collection whose element type was never observed. It is the bottom of
  the type lattice (the narrowest list type). It unifies with any other list and keeps the element type of that
  list. In the example, the first row infers `tags: list<string>`. A merge with the empty array of the second row
  yields `list<?string>`. The element is still a string, but it is no longer known to be present in every row.
- If the element types of an array do not unify, the array detects as `array<mixed>`. That is `json` in the
  schema, because no narrower type fits. `[1, 2, 'a']` and `[1, true]` are such arrays. `[1, 1.5]` is not, because
  integer and float unify to `float`, which gives `list<float>`.
- Some sources transport everything as text (CSV, JSON). They read each cell as a string and narrow it to the
  richest type that parses. A CSV column of `10.5` therefore infers as `?float`, not `?string`.

Inference is a fallback. If the source schema is known, declare it on the extractor ("Declaring the Source Schema"
below). If it is not known, the extractor infers one, also from a plain PHP array ("Inferring Types from an Array"
below).

## When Two Types Disagree

A column holds exactly one type. If two rows disagree, the schema merge widens them to the narrowest type that can
hold both. The result is always one of these:

| both sides are                                  | result   |
|-------------------------------------------------|----------|
| containers (`json`, `list`, `map`, `structure`) | `json`   |
| anything else                                   | `string` |

```php
data_frame()
    ->read(from_array([['a' => 1], ['a' => true]]))
    ->schema();

// a: ?string
```

A union is not a column type. `null|T` is the single exception, and it does not mean "either". It means a nullable
column of `T`. When the definition is built, any other union is refused:

```php
definition_from_type('value', type_union(type_string(), type_null()));
// StringDefinition, nullable

definition_from_type('value', type_union(type_string(), type_integer()));
// UnsupportedUnionTypeException: Column "value" cannot be typed as "integer|string": a column holds
// exactly one type. Only "null|T" is a valid union - that is a nullable column.
// Possible fixes:
// * Declare the widest common type: str_schema('value')
// * Declare json_schema('value') when the shape is genuinely dynamic

list_schema('tags', type_list(type_union(type_string(), type_null())));
// list<?string>

list_schema('tags', type_list(type_union(type_string(), type_integer())));
// UnsupportedUnionTypeException: Column "tags" cannot hold elements of type "integer|string": ...
```

## Declaring the Source Schema

Most extractors accept a schema before the read. Use the fluent `withSchema(Schema)`, or the `$schema` argument that
the `from_*` DSL function can take. The declared types replace inference. Each batch keeps only the columns that the
schema names.

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

This also works for in-memory data. If you declare `tags` as a list, it keeps the element type that the inference
example above threw away:

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

The HTTP extractors accept a schema the same way.
[Typing the row with a schema](/documentation/components/adapters/http.md) shows how to declare the response body as
a `structure`.

Self-descriptive formats carry their schema in the file. In this repository, these formats are Parquet and Floe.
Their extractors read the schema from the file. If `withSchema()` declares a schema, they use it and read every file
by name.

### PostgreSQL sources describe themselves, and keep `withSchema()`

The `from_pgsql_cursor()`, `from_pgsql_limit_offset()` and `from_pgsql_key_set()` extractors derive their schema from
the database. The type mapping of a driver loses information, and a file format does not. For this reason, these
sources keep `withSchema()` as an override that always wins:

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

- It executes a zero-row probe (a run that returns no rows) of your query
  (`SELECT * FROM (<your sql>) flow_describe LIMIT 0`). Then it reads the column metadata of the result. It describes
  the *result*, never the catalog. A view, an alias and a computed column therefore all describe correctly.
- A parameterised query describes normally. The probe binds `null` at every parameter position, so it never sees
  the values that you passed.
- Every derived column is nullable. The result metadata cannot prove that a column is `NOT NULL`. An outer join or
  an expression column can always produce `null`.
- The schema is derived once per read, before the first row. Every batch matches it.

If a query cannot be read or described, nothing is read:

| What is wrong                                                                                                                       | What you get                                                                             |
|-------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| the SQL does not parse                                                                                                              | `ParserException`, before any query runs                                                 |
| not exactly one read-only `SELECT` or `VALUES` (`INSERT ... RETURNING`, two statements, a data-modifying `WITH`, `SELECT ... INTO`) | `InvalidArgumentException`, before any query runs                                        |
| a table, column, function, type or privilege the query names is missing                                                             | the `QueryException` of PostgreSQL, with its position in your SQL, as the read throws it |
| any other refusal of the zero-row probe                                                                                             | `SchemaNotDerivableException`, `getPrevious()` is PostgreSQL's error                     |
| a column type Flow has no type for (`record`, `point`, `line`, `lseg`, `box`, `path`, `polygon`, `circle`)                          | `SchemaNotDerivableException`                                                            |

`->withSchema(...)` skips the probe. This helps with the last row of the table. If the query runs as written, it also
helps with the row before it.

### Doctrine DBAL sources describe themselves, and keep `withSchema()`

`from_dbal_query()`, `from_dbal_queries()`, `from_dbal_limit_offset()`, `from_dbal_limit_offset_qb()` and
`from_dbal_key_set_qb()` derive their schema from a probe of the SQL of the extractor. The extractor keeps the
probe result for the whole read. The probe describes the *result*, never the catalog. A view, an alias and a
computed column therefore describe correctly. The paging of a query builder is never part of what is described.
`withSchema()` stays as an override that always wins and costs no query.

How the probe runs depends on what the driver can answer:

- MySQL prepares your SQL as written and reads the statement's own metadata. Nothing is executed.
- PostgreSQL and SQLite run a zero-row query, `SELECT * FROM (<your sql>) flow_describe WHERE 1=0`.

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

- PostgreSQL (`pgsql`) and MySQL (`mysqli`) report real column types, mapped to Flow types. Every derived
  column is nullable. MySQL types come from a prepared statement, so its type probe executes nothing.
- SQLite (`sqlite3`, `pdo_sqlite`) has no result types per column. Every column therefore describes as nullable
  `string`, and the read casts its values to match.
- A parameterised query describes normally. The `:name` and `?` placeholders of DBAL are rewritten to the dialect
  of the driver. The probe binds `null` at every position (an empty list for an array-typed parameter), so it never
  sees the values that you passed.
- Duplicate output column names collapse to the last one on PostgreSQL and MySQL, the same as the row collapses
  them. An example is `SELECT * FROM a JOIN b ON a.id = b.id` where both tables have `id`. On SQLite, the zero-row
  probe makes the driver rename the second to `id:1`. The schema then has a column that the rows do not have. If a
  SQLite query selects a name twice, give the columns different aliases or declare the schema.

If a query cannot be described, nothing is read:

| What is wrong                                                                                                                                                                                                | What you get                                                                                                      |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| a table or column the query names is missing                                                                                                                                                                 | `TableNotFoundException` / `InvalidFieldNameException` of DBAL (SQLite: `DriverException`), as the read throws it |
| any other refusal (a syntax error, a multi-statement query on SQLite or PostgreSQL)                                                                                                                          | `SchemaNotDerivableException`, `getPrevious()` is the DBAL exception                                              |
| a driver without a result-type probe: `pdo_pgsql` and `pdo_mysql` give a plain `PDO`                                                                                                                         | `SchemaNotDerivableException`. Use the native `pgsql://` or `mysqli://` DSN.                                      |
| a column whose driver type has no Flow type. On PostgreSQL over this route: arrays, `money`, `inet`, ranges and `interval` (the PostgreSQL adapter above maps more of them). On MySQL: `BIT` and `GEOMETRY`. | `SchemaNotDerivableException`                                                                                     |

`->withSchema(...)` skips the probe. This helps with the last two rows of the table. If the query runs as written, it
also helps with the second row.

## Inferring Types from an Array

An array source has no schema of its own, so ask it to infer one:

```php
<?php

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\infer_schema;

data_frame()
    ->read(from_array([['id' => '1', 'customer' => 'Norbert', 'total' => '10.5', 'paid' => 'true']])
        ->inferSchema(infer_schema()))
    ->printSchema();

// schema
// |-- id: ?string
// |-- customer: ?string
// |-- total: ?string
// |-- paid: ?string
```

`from_array()` infers from the PHP value, so a string cell stays a string. Only file sources narrow text to a richer
type. `from_csv()` and `from_json()` read each cell as text and narrow it to the richest type that parses. A
`'10.5'` from a CSV therefore infers as `?float`.

If you know the types, a declared schema ("Declaring the Source Schema" above) is still the stronger option. If you
do not know them, inference gives the answer. Every inferred column is nullable. Inference never claims that a column
cannot be null.

## Schema Validation Strategies

Flow PHP has three built-in validation strategies:

- [StrictValidator](/src/core/etl/src/Flow/ETL/Values/Schema/Validator/StrictValidator.php): rows must match the schema
  exactly. Extra columns make the validation fail.
- [SelectiveValidator](/src/core/etl/src/Flow/ETL/Values/Schema/Validator/SelectiveValidator.php): only the columns that the
  schema defines must match. It ignores extra columns.
- [EvolvingValidator](/src/core/etl/src/Flow/ETL/Values/Schema/Validator/EvolvingValidator.php): allows missing and extra
  nullable columns.

By default, DataFrame uses `StrictValidator`. To use a different validator, give it as the second parameter of
`DataFrame::match()`.

## Basic Schema Matching

To match data against a schema, use `DataFrame::match()`. `from_array()` infers every column as nullable, so the
expected schema declares them nullable too:

```php 
<?php

use Flow\ETL\IO\Loader\StreamLoader\Output;
use Flow\ETL\Values\Schema\Metadata;

use function Flow\ETL\DSL\{data_frame, from_array, schema, int_schema, str_schema, bool_schema, to_output};

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Product 1', 'active' => true],
        ['id' => 2, 'name' => 'Product 2', 'active' => false],
        ['id' => 3, 'name' => 'Product 3', 'active' => true]
    ]))
    ->match(
        schema(
            int_schema('id', nullable: true),
            str_schema('name', nullable: true),
            bool_schema('active', nullable: true, metadata: Metadata::empty()->add('key', 'value')),
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
            int_schema('id', nullable: true),
            str_schema('name', nullable: true)
        ),
        schema_selective_validator() // Only validate id and name, ignore other fields
    )
    ->write(to_output())
    ->run();
```