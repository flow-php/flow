---
package: flow-php/etl-adapter-csv
---

# CSV Adapter 

[PACKAGE_NAV]

[TOC]

The CSV adapter reads and writes CSV data in ETL (Extract, Transform, Load) pipelines.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/etl-adapter-csv.md).

## Extractor

```php
<?php

$rows = data_frame()
    ->read(from_csv($path))
    ->fetch();
```

### Header and missing cells

```
header row  [' id ', '', 'name']   ->  columns ['id', 'e01', 'name']   (trimmed, a blank cell named e + its position)
record      ['1', 'a']             ->  corrupted record (2 cells, the header has 3); `withMissingColumnsAsNull(true)` makes 'name' null
record      ['1', 'a', 'x', '']    ->  corrupted record (a non-empty cell past the header), an empty one is dropped
record      ['1', '', 'x']         ->  'e01' => null   (a present '' is null; `withEmptyToNull(false)` keeps it '')
empty line                         ->  skipped
```

A corrupted record throws `CorruptedRecordException`, or goes to `->withCorruptedRecords($loader)`. For more
information, read [Corrupted Records](/documentation/components/core/corrupted-records.md). If a column of
`->withSchema()` is not in the header, the read throws `SchemaValidationException`.

## Loader

```php
<?php

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{bool_schema, data_frame, datetime_schema, float_schema, from_array, int_schema, json_schema, list_schema, map_schema, schema, structure_schema};
use function Flow\Types\DSL\{type_date, type_integer, type_list, type_map, type_string, type_structure};

data_frame()
    ->read(from_array(
        [
            ['id' => 1, 'active' => true, 'price' => 0.1 + 0.2, 'at' => new DateTimeImmutable('2026-01-02 03:04:05 UTC'), 'days' => [new DateTimeImmutable('2026-01-02')], 'labels' => [0 => 'a', 1 => 'b'], 'size' => ['w' => 1, 'h' => 2], 'meta' => '{"tags": []}'],
            ['id' => 2, 'active' => false, 'price' => 1.0, 'at' => null, 'days' => [], 'labels' => [], 'size' => ['w' => 3, 'h' => 4], 'meta' => '{}'],
        ],
        schema(
            int_schema('id'), bool_schema('active'), float_schema('price'), datetime_schema('at', nullable: true),
            list_schema('days', type_list(type_date())), map_schema('labels', type_map(type_integer(), type_string())),
            structure_schema('size', type_structure(['w' => type_integer(), 'h' => type_integer()])), json_schema('meta'),
        ),
    ))
    ->write(to_csv($path))
    ->run();
```

```csv
id,active,price,at,days,labels,size,meta
1,true,0.30000000000000004,2026-01-02T03:04:05+00:00,"[""2026-01-02""]","{""0"":""a"",""1"":""b""}","{""w"":1,""h"":2}","{""tags"": []}"
2,false,1.0,,[],{},"{""w"":3,""h"":4}",{}
```

| Column                     | Field                                                                                                                                   |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `boolean`                  | `true` / `false`                                                                                                                        |
| `float`                    | the shortest text that reads back as the same float, always with a fraction or an exponent: `1.0`, `1.0e+25`. Also `NAN`, `INF`, `-INF` |
| `null`, `''`               | an empty field. `from_csv()` reads both back as `null`, except with `withEmptyToNull(false)`                                            |
| `datetime`, `date`         | `withDateTimeFormat()` (default `DATE_ATOM`) in the column zone, `withDateFormat()` (default `Y-m-d`)                                   |
| `time`                     | microseconds                                                                                                                            |
| `list`, `map`, `structure` | JSON: a list is `[...]`, and a map and a structure are always `{...}`. Each element is written like a column of its type                |
| `json`                     | the stored text                                                                                                                         |

Floats are rendered under `serialize_precision = -1`. If the ini has a different value, the writer sets it for the
render and then restores it. `ini_set()` must therefore be allowed. If it is not allowed, the writer throws
`RuntimeException('Writing floats requires serialize_precision = -1 and ini_set() cannot change it')`.

When the [`flow_php`](/documentation/components/extensions/flow-php-ext.md) extension is loaded, it renders the same
bytes natively.
