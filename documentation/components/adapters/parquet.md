---
package: flow-php/etl-adapter-parquet
---

# ETL Adapter: Parquet

[PACKAGE_NAV]

Flow PHP's Adapter Parquet is a sophisticated library meticulously engineered to enable seamless interaction with
Parquet data formats within your ETL (Extract, Transform, Load) workflows. This adapter is crucial for developers
looking to efficiently extract from or load data into Parquet formats, ensuring a streamlined and reliable data
transformation process. By employing the Adapter Parquet library, developers can access a robust set of features
designed for precise Parquet data handling, making complex data transformations both manageable and efficient. The
Adapter Parquet library encapsulates a comprehensive set of functionalities, providing a streamlined API for engaging
with Parquet data, which is indispensable in modern data processing and transformation environments. This library
embodies Flow PHP's commitment to providing versatile and effective data processing solutions, making it a prime choice
for developers dealing with Parquet data in large-scale and data-intensive scenarios. With Flow PHP's Adapter Parquet,
managing Parquet data within your ETL workflows becomes a more simplified and efficient task, perfectly aligning with
the robust and adaptable nature of the Flow PHP ecosystem.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-parquet.md).

## Engines

Without `engine:`, Parquet is read and written by the [`flow_php` extension](/documentation/components/extensions/flow-php-ext.md)
when it is loaded, else by `AdaptiveParquetEngine`. An explicit engine is always used:

```php
<?php

use Flow\Parquet\Engine\PhpParquetEngine;

use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\df;

df()
    ->read(from_parquet(__DIR__ . '/orders.parquet', engine: new PhpParquetEngine()))
    ->write(to_parquet(__DIR__ . '/copy.parquet', engine: new PhpParquetEngine()))
    ->run();
```

Flow `float` columns are written as Parquet `DOUBLE`.

## Schema

The file footer is the schema: `from_parquet(...)->withSchema()` throws. Project with `columns:`, change types after
reading:

```php
<?php

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{data_frame, ref, to_output, to_timezone};
use function Flow\Types\DSL\type_json;

data_frame()
    ->read(from_parquet(__DIR__ . '/orders.parquet', columns: ['id', 'payload', 'created_at']))
    ->withEntry('payload', ref('payload')->cast(type_json()))
    ->withEntry('created_at', to_timezone(ref('created_at'), 'Europe/Warsaw'))
    ->write(to_output())
    ->run();
```
