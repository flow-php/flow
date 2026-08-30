# Building Blocks

[DOC_LINK:/documentation/components/core/core.md]

Columns of the [Data Frame](/documentation/components/core/core.md) are described by the
[Schema](/src/core/etl/src/Flow/ETL/Schema.php) - it owns their names, order, types and nullability.
A [Row](/src/core/etl/src/Flow/ETL/Row.php) carries the values for one record, keyed by column name.
A group of Rows is called `Rows`, represented by the [Rows](/src/core/etl/src/Flow/ETL/Rows.php)
class, and every `Rows` carries the one `Schema` that describes it.

Let's look at the following example:

```php
<?php

declare(strict_types=1);

use function Flow\ETL\DSL\{bool_schema, int_schema, row, rows, schema, str_schema};

$rows = rows(
    schema(int_schema('id'), str_schema('name'), bool_schema('active')),
    row(['id' => 1, 'name' => 'user_01', 'active' => true]),
    row(['id' => 2, 'name' => 'user_02', 'active' => false]),
    row(['id' => 3, 'name' => 'user_03', 'active' => true]),
    row(['id' => 4, 'name' => 'user_04', 'active' => false]),
);
```

Rows are the main data structure in Flow ETL, they're used to represent data in the data frame.
Extractors are yielding Rows and Loaders are saving Rows.

The same can be achieved using the following code:

```php
<?php

declare(strict_types=1);

use function Flow\ETL\DSL\array_to_rows;

$rows = array_to_rows([
    ['id' => 1, 'name' => 'user_01', 'active' => true],
    ['id' => 2, 'name' => 'user_02', 'active' => false],
    ['id' => 3, 'name' => 'user_03', 'active' => true],
    ['id' => 4, 'name' => 'user_04', 'active' => false],
]);
```

## Column Types

Every column is described by a [Definition](/src/core/etl/src/Flow/ETL/Schema/Definition.php), built with
the matching `*_schema()` [DSL function](/src/core/etl/src/Flow/ETL/DSL/functions.php). A definition owns
the column name, its [Flow Type](/documentation/components/libs/types.md), nullability and metadata.

| Column | DSL function | Definition |
|--------|--------------|------------|
| Boolean | `bool_schema()` | [BooleanDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/BooleanDefinition.php) |
| Date | `date_schema()` | [DateDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/DateDefinition.php) |
| DateTime | `datetime_schema()` | [DateTimeDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/DateTimeDefinition.php) |
| Enum | `enum_schema()` | [EnumDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/EnumDefinition.php) |
| Float | `float_schema()` | [FloatDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/FloatDefinition.php) |
| HTML | `html_schema()` | [HTMLDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/HTMLDefinition.php) |
| HTML Element | `html_element_schema()` | [HTMLElementDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/HTMLElementDefinition.php) |
| Integer | `int_schema()`, `integer_schema()` | [IntegerDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/IntegerDefinition.php) |
| Json | `json_schema()` | [JsonDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/JsonDefinition.php) |
| List | `list_schema()` | [ListDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/ListDefinition.php) |
| Map | `map_schema()` | [MapDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/MapDefinition.php) |
| Null | `null_schema()` | [NullDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/NullDefinition.php) |
| String | `str_schema()`, `string_schema()` | [StringDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/StringDefinition.php) |
| Structure | `structure_schema()` | [StructureDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/StructureDefinition.php) |
| Time | `time_schema()` | [TimeDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/TimeDefinition.php) |
| Time Zone | `time_zone_schema()` | [TimeZoneDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/TimeZoneDefinition.php) |
| Union | `union_schema()` | [UnionDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/UnionDefinition.php) |
| Uuid | `uuid_schema()` | [UuidDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/UuidDefinition.php) |
| XML | `xml_schema()` | [XMLDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/XMLDefinition.php) |
| XML Element | `xml_element_schema()` | [XMLElementDefinition](/src/core/etl/src/Flow/ETL/Schema/Definition/XMLElementDefinition.php) |

When no schema is given up front - as in the `array_to_rows()` example above - a
[Hydrator](/src/core/etl/src/Flow/ETL/Row/Hydrator.php) infers one from the whole batch and turns the raw
values into `Rows`. Inference looks at every row of the batch, not at a single value, so one column always
ends up with one type.
