---
package: flow-php/etl-adapter-json
---

# ETL Adapter: JSON

[PACKAGE_NAV]

[TOC]

Flow PHP's Adapter JSON is a meticulously engineered library aimed at facilitating seamless interactions with JSON data
within your ETL (Extract, Transform, Load) workflows. This adapter is paramount for developers seeking to effortlessly
extract from or load data into JSON formats, ensuring a fluid and reliable data transformation experience. By utilizing
the Adapter JSON library, developers can harness a robust set of features tailored for precise JSON data handling,
making complex data transformations both manageable and efficient. The Adapter JSON library encapsulates a comprehensive
set of functionalities, providing a streamlined API for engaging with JSON data, which is indispensable in modern data
processing and transformation scenarios. This library embodies Flow PHP's commitment to offering versatile and efficient
data processing solutions, making it a prime choice for developers dealing with JSON data in large-scale and
data-intensive environments. With Flow PHP's Adapter JSON, managing JSON data within your ETL workflows becomes a more
simplified and efficient task, perfectly aligning with the robust and adaptable nature of the Flow PHP ecosystem.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-json.md).


> Json library is not explicitly required, you need to make sure it is available in your composer.json file.
> If you are only using Loader, this dependency is optional.

## Extractor - JSONMachine - JsonExtractor

```php
<?php

use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{data_frame, to_output};

data_frame()
    ->read(from_json(__DIR__ . '/data.json'))
    ->collect()
    ->write(to_output())
    ->run();
```

## Loader - JsonLoader

```php
<?php

use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array(
        \array_map(
            fn (int $i) : array => ['id' => $i, 'name' => 'name_' . $i],
            \range(0, 10)
        )
    ))
    ->collect()
    ->write(to_json(\sys_get_temp_dir() . '/file.json'))
    ->run();
```

## Loader - JsonLoader - JSON lines

It is also possible to export the rows using the [json lines](https://jsonlines.org/) format


```php
<?php

use function Flow\ETL\Adapter\JSON\to_json_lines;
use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array(
        \array_map(
            fn (int $i) : array => ['id' => $i, 'name' => 'name_' . $i],
            \range(0, 10)
        )
    ))
    ->collect()
    ->write(to_json_lines(\sys_get_temp_dir() . '/file.jsonl'))
    ->run();
```
