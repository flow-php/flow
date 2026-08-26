# Schema

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Schema defines the structure and validation rules for DataFrame data. It provides type safety, data validation, and
metadata management for your data processing pipelines.

## Understanding Schema Components

A schema consists of entry definitions that specify:

- **Entry Name**: The column/field identifier
- **Type**: The expected data type (class string)
- **Nullable**: Whether NULL values are permitted
- **Metadata**: Key-value pairs for additional context

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

When no schema is declared, every value gets its type detected as rows are created, and each batch carries its own
schema. `DataFrame::schema()` runs the pipeline and merges the per-batch schemas into one; `printSchema()` prints them.

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

## Automatic Casting

`DataFrame::autoCast()` detects every value in the pipeline: strings are narrowed to `null`, `boolean`, `integer`,
`float`, `datetime`, `date`, `uuid`, `json` or `xml` (and `html` on PHP 8.4+) when they parse as one, and arrays
mixing integers with floats are unified to floats. Use it when the source has no schema to declare - typically text formats:

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\data_frame;

data_frame()
    ->read(from_csv(__DIR__ . '/orders.csv'))
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