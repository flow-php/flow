---
package: flow-php/etl-adapter-parquet
---

# ETL Adapter: Parquet

[PACKAGE_NAV]

The Parquet adapter reads and writes Parquet files in ETL (Extract, Transform, Load) pipelines.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/etl-adapter-parquet.md).

## Engines

Without `engine:`, `AdaptiveParquetEngine` opens the file. The loaded extensions, arrow-ext and
[`flow_php`](/documentation/components/extensions/flow-php-ext.md), decide the lane:

| arrow-ext | `flow_php` | Engine              | Batches                                                              |
|-----------|------------|---------------------|----------------------------------------------------------------------|
| no        | no         | `PhpParquetEngine`  | PHP columns                                                          |
| yes       | no         | `RustParquetEngine` | PHP columns built from the values of arrow-ext                       |
| no        | yes        | `PhpParquetEngine`  | native columns built from the values of the PHP engine               |
| yes       | yes        | `RustParquetEngine` | batches of arrow-ext directly into native columns, and back on write |

`RustParquetEngine` needs little-endian byte order. Native columns come from the
[column backend](/documentation/components/core/column-backend.md). An explicit engine is always used.
`PhpParquetEngine` does not use arrow-ext:

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

The file footer is the schema. Select columns with `columns:`, and change types after the read:

```php
<?php

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{data_frame, ref, to_output, to_timezone};
use function Flow\Types\DSL\type_json;

data_frame()
    ->read(from_parquet(__DIR__ . '/orders.parquet', columns: ['id', 'payload', 'created_at']))
    ->withColumn('payload', ref('payload')->cast(type_json()))
    ->withColumn('created_at', to_timezone(ref('created_at'), 'Europe/Warsaw'))
    ->write(to_output())
    ->run();
```

A `UINT_64` column reads as `numeric_string` (`'18446744073709551615'` is larger than `PHP_INT_MAX`). A
`numeric_string` column is written as a parquet `STRING`. To write `UINT_64`, use the lib with an explicit schema:

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

### Declared schema

```php
<?php

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{data_frame, int_schema, schema, str_schema, to_output};

data_frame()
    ->read(from_parquet(__DIR__ . '/orders/**/*.parquet')->withSchema(schema(
        int_schema('id'),
        str_schema('name', nullable: true),
    )))
    ->select('id')
    ->write(to_output())
    ->run();
```

- A glob that matches no file reads 0 rows. Without `withSchema()`, it throws `InferredSchemaException`.
- Columns match by name. A file column that is not declared is not read. A declared nullable column that a file does
  not have is `null`. A declared non-nullable column that a file does not have fails.

## Filter pushdown

A `filter()` directly after `from_parquet()` moves into the reader, which returns only matching rows. The reader
skips row groups with statistics that prove that no row matches. A `limit()` after the filter stops the read at that
number of matching rows:

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

The reader takes each part of an `and()` separately. The pushed parts compare a file column with a literal that the reader can compare exactly.

Pushed:

- `equals`, `notEquals`, `same`, `notSame`
- `lessThan`, `lessThanEqual`, `greaterThan`, `greaterThanEqual`
- `between`, `isIn` (literal list), `startsWith`, `isNull`, `isNotNull`
- `not()`/`and()`/`or()` of those
- an int literal on a float column up to 2^53, an integral float on an int column below 2^53
- a `date` literal on a `datetime` column and the reverse
- a digit string (`lit('1000')`) on a `numeric_string` (`UINT_64`) column

Evaluated on the rows read:

- other functions, two columns compared, a literal of another type
- `json` columns (except `isNull`/`isNotNull`), ordering on `uuid`
- a column that the file stores as a different type than the read type

Partition columns do not filter rows. They list fewer files ([partitioning](/documentation/components/core/partitioning.md)).

`explain()` shows the filter and the limit under `Read`. It does not show what the reader skipped:

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
