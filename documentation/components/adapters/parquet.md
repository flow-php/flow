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

Without `engine:`, `AdaptiveParquetEngine` opens the file. The loaded extensions, arrow-ext and
[`flow_php`](/documentation/components/extensions/flow-php-ext.md), pick the lane:

| arrow-ext | `flow_php` | Engine              | Batches                                                             |
|-----------|------------|---------------------|---------------------------------------------------------------------|
| no        | no         | `PhpParquetEngine`  | PHP columns                                                         |
| yes       | no         | `RustParquetEngine` | PHP columns built from arrow-ext's values                           |
| no        | yes        | `PhpParquetEngine`  | native columns built from the PHP engine's values                   |
| yes       | yes        | `RustParquetEngine` | arrow-ext's batches straight into native columns, and back on write |

`RustParquetEngine` needs little-endian byte order. Native columns come from the
[column backend](/documentation/components/core/column-backend.md). An explicit engine is always used;
`PhpParquetEngine` opts out of arrow-ext:

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

A `UINT_64` column reads as `numeric_string` (`'18446744073709551615'` is past `PHP_INT_MAX`). A `numeric_string`
column is written as a parquet `STRING`; write `UINT_64` through the lib with an explicit schema:

```php
<?php

use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};
use Flow\Parquet\Writer;

(new Writer())->write(
    __DIR__ . '/ids.parquet',
    Schema::with(new FlatColumn('id', PhysicalType::INT64, logicalType: LogicalType::integer(64, false))),
    [['id' => 7], ['id' => '18446744073709551615']],
);
```

## Filter pushdown

A `filter()` directly over `from_parquet()` moves into the reader, which returns only matching rows: row groups whose
statistics prove that no row matches are skipped, and a `limit()` after the filter stops the read at that many
matching rows:

```php
<?php

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{data_frame, lit, ref, to_output};

data_frame()
    ->read(from_parquet(__DIR__ . '/orders/*.parquet'))
    ->filter(ref('id')->greaterThan(lit(1000))->and(ref('status')->equals(lit('paid'))))
    ->limit(10)
    ->write(to_output())
    ->run();
```

Each part of an `and()` is pushed on its own when it compares a file column with a literal it can compare exactly.

Pushed:

- `equals`, `notEquals`, `same`, `notSame`, `lessThan`, `lessThanEqual`, `greaterThan`, `greaterThanEqual`,
  `between`, `isIn` (literal list), `startsWith`, `isNull`, `isNotNull`, and `not()`/`and()`/`or()` of those
- an int literal on a float column up to 2^53, an integral float on an int column below 2^53
- a `date` literal on a `datetime` column and the reverse
- a digit string (`lit('1000')`) on a `numeric_string` (`UINT_64`) column

Evaluated on the rows read:

- other functions, two columns compared, a literal of another type
- `json` columns (except `isNull`/`isNotNull`), ordering on `uuid`
- a column the file stores as another type than the read

Partition columns list fewer files instead ([partitioning](/documentation/components/core/partitioning.md)).

`explain()` shows the filter and the limit under `Read`, not what the reader skipped:

```
#3 Result
│  Rows this plan hands out: to the trigger, or to the node reading it
└─ #2 Limit
   │  Limit: 10
   └─ #1 Read
         Extractor: ParquetExtractor
         Source: file:///orders/*.parquet
         Limit: 10
         Filter: id > 1000
         Filter: status = 'paid'
```
