# Schema

- [⬅️️ Back](core.md)

Schema is a set of rules that defines how data is structured.
It is used to validate data and to provide information about data structure.

Before loading data to sink it might be a good idea to validate it against the schema.
Row Schema is built from Entry Definitions, each definition is created from:

* `entry` - name of entry
* `type` - type of entry (class string)
* `nullable` - if `true` NullEntry with matching name will also pass the validation regardless of the type
* `constraint` - additional, flexible validation. Useful for checking if entry value is for example one of expected values
* `metadata` - additional key-value collection that can carry additional context for the definition

There is more than one way to validate the schema, built in strategies are defined below:

* [StrictValidator](../../../src/core/etl/src/Flow/ETL/Row/Schema/StrictValidator.php) - each row must exactly match the schema, extra entries will fail validation
* [SelectiveValidator](../../../src/core/etl/src/Flow/ETL/Row/Schema/SelectiveValidator.php) - only rows defined in the schema must match, any extra entry in row will be ignored

By default, ETL is initializing `StrictValidator`, but it's possible to override it by passing second argument to `DataFrame::validate()` method.

## Example - schema validation

```php 
<?php

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Product 1', 'active' => true],
        ['id' => 2, 'name' => 'Product 2', 'active' => false],
        ['id' => 3, 'name' => 'Product 3', 'active' => true]
    ]))
    ->validate(
        schema(
            int_schema('id', $nullable = false),
            str_schema('name', $nullable = true),
            bool_schema('active', $nullable = false, Metadata::empty()->add('key', 'value')),
        )
    )
    ->write(to_output(false, Output::rows_and_schema))
    ->run();
```

Output:

```console
Fatal error: Uncaught Flow\ETL\Exception\SchemaValidationException: Given schema:
schema
|-- id: integer
|-- name: ?string
|-- active: boolean

Does not match rows:
schema
|-- id: integer
|-- name: string
|-- active: boolean
```

## Example - display schema

```php
<?php

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Product 1', 'active' => true, 'tags' => ['tag1', 'tag2']],
        ['id' => 2, 'name' => 'Product 2', 'active' => false, 'address' => ['city' => 'London', 'country' => 'UK']],
        ['id' => 3, 'name' => 'Product 3', 'active' => true, 'tags' => ['tag1', 'tag2']],
        ['id' => 3, 'name' => 'Product 3', 'active' => true]
    ]))
    ->collect()
    ->write(to_output(false, Output::schema))
    ->run();
```

Output:

```console
schema
|-- id: integer
|-- name: string
|-- active: boolean
|-- tags: list<string>
|-- address: structure
|    |-- city: string
|    |-- country: string
```