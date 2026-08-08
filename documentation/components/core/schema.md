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
union_schema('tags', type_union(type_string(), type_array()))->type()->toString();   // "json|string"
```

Declare the concrete shape whenever it is known - `type_list()`, `type_map()` or `type_structure()` keep element typing
that `json` throws away, and adapters can map them onto native nested types.

## Schema Validation Strategies

Flow PHP provides two built-in validation strategies:

- **[StrictValidator](/src/core/etl/src/Flow/ETL/Row/Schema/StrictValidator.php)** - Rows must exactly match the schema;
  extra entries cause validation failure
- **[SelectiveValidator](/src/core/etl/src/Flow/ETL/Row/Schema/SelectiveValidator.php)** - Only validates entries
  defined in schema; ignores extra entries

By default, DataFrame uses `StrictValidator`, but you can specify a different validator as the second parameter to
`DataFrame::match()`.

## Basic Schema Matching

Use `DataFrame::match()` to validate data against a schema:

```php 
<?php

use function Flow\ETL\DSL\{data_frame, from_array, schema, int_schema, str_schema, bool_schema, to_output};
use Flow\ETL\Row\Schema\Metadata;

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