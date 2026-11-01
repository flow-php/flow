---
package: flow-php/etl-adapter-text
---

# ETL Adapter: Text

[PACKAGE_NAV]

[TOC]

The Text adapter reads and writes text data in ETL (Extract, Transform, Load) pipelines.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/etl-adapter-text.md).

## Extractor

```php
<?php

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\data_frame;

$rows = data_frame()
    ->read(from_text(__DIR__ . '/file.txt'))
    ->fetch();
```

Each line is one row with one `string` column `text`.

## Loader

The batch must have one column or no column.

```php
<?php

use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array([['name' => 'Norbert'], ['name' => 'Tomek'], ['name' => 'Dawid']]))
    ->write(to_text(__DIR__ . '/names.txt'))
    ->run();
```

Each value of the one column is one line:

```php
<?php

use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\{data_frame, float_schema, from_array, schema};

data_frame()
    ->read(from_array([['price' => 0.1 + 0.2], ['price' => 1.0], ['price' => INF]], schema(float_schema('price'))))
    ->write(to_text($path))
    ->run();
```

```
0.30000000000000004
1.0
INF
```

| Column                     | Line                                                                                                                                    |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `boolean`                  | `true` / `false`                                                                                                                        |
| `float`                    | the shortest text that reads back as the same float, always with a fraction or an exponent: `1.0`, `1.0e+25`. Also `NAN`, `INF`, `-INF` |
| `null`                     | an empty line                                                                                                                           |
| `datetime`, `date`         | `DATE_ATOM` in the column zone, `Y-m-d`                                                                                                 |
| `time`                     | microseconds                                                                                                                            |
| `json`                     | the stored text                                                                                                                         |
| `list`, `map`, `structure` | refused: `Text data loader supports only scalar values, got array`                                                                      |

Floats are rendered under `serialize_precision = -1`. If the ini has a different value, the writer sets it for the
render and then restores it. `ini_set()` must therefore be allowed. If it is not allowed, the writer throws
`RuntimeException('Writing floats requires serialize_precision = -1 and ini_set() cannot change it')`.
