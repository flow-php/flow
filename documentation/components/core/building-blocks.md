# Building Blocks

[DOC_LINK:/documentation/components/core/core.md]

The [Schema](/src/core/etl/src/Flow/ETL/Values/Schema.php) describes the columns of the
[Data Frame](/documentation/components/core/core.md). It owns their names, order, types and nullability.
The [Rows](/src/core/etl/src/Flow/ETL/Values/Rows.php) class represents a batch of records: one column for each
schema definition. Each `Rows` carries the one `Schema` that describes it.

Look at this example:

```php
<?php

declare(strict_types=1);

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

$rows = array_to_rows(
    [
        ['id' => 1, 'name' => 'user_01', 'active' => true],
        ['id' => 2, 'name' => 'user_02', 'active' => false],
        ['id' => 3, 'name' => 'user_03', 'active' => true],
        ['id' => 4, 'name' => 'user_04', 'active' => false],
    ],
    schema(int_schema('id'), str_schema('name'), bool_schema('active')),
);
```

Extractors yield `Rows`, transformers return new `Rows`, and loaders write them. To read a record back, use
`$rows->values($i)` (one row as an array), `$rows->toArray()` (all rows) or `$rows->column('name')->value($i)`.

## Column Types

A [Definition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition.php) describes each column. The matching
`*_schema()` [DSL function](/src/core/etl/src/Flow/ETL/DSL/functions.php) builds it. A definition owns
the column name, its [Flow Type](/documentation/components/libs/types.md), nullability and metadata.

| Column       | DSL function                       | Definition                                                                                             |
|--------------|------------------------------------|--------------------------------------------------------------------------------------------------------|
| Boolean      | `bool_schema()`                    | [BooleanDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/BooleanDefinition.php)         |
| Date         | `date_schema()`                    | [DateDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/DateDefinition.php)               |
| DateTime     | `datetime_schema()`                | [DateTimeDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/DateTimeDefinition.php)       |
| Enum         | `enum_schema()`                    | [EnumDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/EnumDefinition.php)               |
| Float        | `float_schema()`                   | [FloatDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/FloatDefinition.php)             |
| HTML         | `html_schema()`                    | [HTMLDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/HTMLDefinition.php)               |
| HTML Element | `html_element_schema()`            | [HTMLElementDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/HTMLElementDefinition.php) |
| Integer      | `int_schema()`, `integer_schema()` | [IntegerDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/IntegerDefinition.php)         |
| Json         | `json_schema()`                    | [JsonDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/JsonDefinition.php)               |
| List         | `list_schema()`                    | [ListDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/ListDefinition.php)               |
| Map          | `map_schema()`                     | [MapDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/MapDefinition.php)                 |
| Null         | `null_schema()`                    | [NullDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/NullDefinition.php)               |
| String       | `str_schema()`, `string_schema()`  | [StringDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/StringDefinition.php)           |
| Structure    | `structure_schema()`               | [StructureDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/StructureDefinition.php)     |
| Time         | `time_schema()`                    | [TimeDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/TimeDefinition.php)               |
| Time Zone    | `time_zone_schema()`               | [TimeZoneDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/TimeZoneDefinition.php)       |
| Uuid         | `uuid_schema()`                    | [UuidDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/UuidDefinition.php)               |
| XML          | `xml_schema()`                     | [XMLDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/XMLDefinition.php)                 |
| XML Element  | `xml_element_schema()`             | [XMLElementDefinition](/src/core/etl/src/Flow/ETL/Values/Schema/Definition/XMLElementDefinition.php)   |

A `datetime` column has one time zone, UTC by default: `datetime_schema('at', zone: 'Europe/Warsaw')`. At the
admission of the row, a value in a different zone is converted to it. The instant does not change.

A column holds exactly one type. `null|T` writes a nullable column, and a nullable list element, map value or
structure element. All other unions are refused. Declare the widest common type with `str_schema()`. For a shape that
is really dynamic, use `json_schema()`.

The schema is declared, never guessed. `array_to_rows()` takes it as its second argument. The
[column backend](/documentation/components/core/column-backend.md) builds `Rows` against it, and casts each value to
the type of its column. A value that the declared type refuses stops the batch with a `SchemaMismatchException`. The
exception names the column and its row.

Schema inference belongs to the readers. They sample a source and derive a schema from it before the first row
flows. For more information, read [Schema](/documentation/components/core/schema.md).
