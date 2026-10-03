---
package: flow-php/etl-adapter-text
---

# ETL Adapter: Text

[PACKAGE_NAV]

[TOC]

Flow PHP's Adapter Text is a meticulously crafted library dedicated to enabling seamless handling of text data within
your ETL (Extract, Transform, Load) workflows. This adapter is pivotal for developers seeking to effortlessly extract
from or load data into text formats, ensuring a fluid and dependable data transformation experience. By employing the
Adapter Text library, developers have access to a robust set of features tailored for precise text data handling,
simplifying complex data transformations and streamlining text data processing tasks. The Adapter Text library
encapsulates an intuitive set of functionalities, offering a streamlined API for engaging with text data, which is
crucial in contemporary data processing and transformation scenarios. This library exemplifies Flow PHP's commitment to
providing versatile and efficient data processing solutions, making it an excellent choice for developers navigating
text data in large-scale and data-intensive projects. With Flow PHP's Adapter Text, managing text data within your ETL
workflows becomes a more refined and efficient endeavor, perfectly aligning with the robust and adaptable framework of
the Flow PHP ecosystem.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-text.md).

## Extractor

```php
<?php

use function Flow\ETL\Adapter\Text\from_text;
use function Flow\ETL\DSL\data_frame;

$rows = data_frame()
    ->read(from_text(__DIR__ . '/file.txt'))
    ->fetch();
```

Each line is one row with a single `string` column `text`.

## Loader

The batch must have at most one column.

```php
<?php

use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array([['name' => 'Norbert'], ['name' => 'Tomek'], ['name' => 'Dawid']]))
    ->write(to_text(__DIR__ . '/names.txt'))
    ->run();
```

Every value of the single column is one line:

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

| Column                     | Line                                                                                                                               |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| `boolean`                  | `true` / `false`                                                                                                                   |
| `float`                    | the shortest text that reads back as the same float, always with a fraction or an exponent: `1.0`, `1.0e+25`; `NAN`, `INF`, `-INF` |
| `null`                     | an empty line                                                                                                                      |
| `datetime`, `date`         | `DATE_ATOM` in the column zone, `Y-m-d`                                                                                            |
| `time`                     | microseconds                                                                                                                       |
| `json`                     | the stored text                                                                                                                    |
| `list`, `map`, `structure` | refused: `Text data loader supports only scalar values, got array`                                                                 |

Floats are rendered under `serialize_precision = -1`. When the ini holds another value the writer sets it for the
render and restores it, so `ini_set()` must be allowed; otherwise it throws
`RuntimeException('Writing floats requires serialize_precision = -1 and ini_set() cannot change it')`.
