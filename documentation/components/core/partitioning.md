# Partitioning

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Partitioning splits written data into `column=value` directories, and lets a reader skip whole
directories it does not need.

Partitioning is declared on the **loader**, not on the DataFrame.

## Basic Partitioning

```php
<?php

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite, partition_by, ref};

data_frame()
    ->read(from_array([
        ['date' => '2024-01-01', 'department' => 'sales', 'amount' => 100],
        ['date' => '2024-01-01', 'department' => 'marketing', 'amount' => 200],
        ['date' => '2024-01-02', 'department' => 'sales', 'amount' => 150],
    ]))
    ->write(to_csv(__DIR__ . '/output/sales.csv')
        ->partitionBy(partition_by(ref('date')))
        ->saveMode(overwrite()))
    ->run();
```

```text
output/
├── date=2024-01-01/
│   └── sales.csv
└── date=2024-01-02/
    └── sales.csv
```

## Multi-Column Partitioning

Each column adds a nesting level.

```php
<?php

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite, partition_by, ref};

data_frame()
    ->read(from_array([
        ['date' => '2024-01-01', 'department' => 'sales', 'amount' => 100],
        ['date' => '2024-01-01', 'department' => 'marketing', 'amount' => 200],
    ]))
    ->write(to_csv(__DIR__ . '/output/sales.csv')
        ->partitionBy(partition_by(ref('date'), ref('department')))
        ->saveMode(overwrite()))
    ->run();
```

```text
output/
└── date=2024-01-01/
    ├── department=sales/
    │   └── sales.csv
    └── department=marketing/
        └── sales.csv
```

## Partition Placeholders

A `{column}` placeholder in the destination path turns that partition into part of the file name
instead of a directory.

```php
<?php

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite, partition_by, ref};

data_frame()
    ->read(from_array([
        ['date' => '2024-01-01', 'department' => 'sales', 'amount' => 100],
        ['date' => '2024-01-01', 'department' => 'marketing', 'amount' => 200],
    ]))
    ->write(to_csv(__DIR__ . '/output/{department}.csv')
        ->partitionBy(partition_by(ref('date'), ref('department')))
        ->saveMode(overwrite()))
    ->run();
```

```text
output/
└── date=2024-01-01/
    ├── sales.csv
    └── marketing.csv
```

Rules:

- Every placeholder must match a partition column, otherwise the write fails.
- A path with placeholders requires partitioned rows.
- Placeholders may appear in any segment and be combined: `/output/{date}/{department}_report.csv`.

### Reading Data Partitioned with Placeholders

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, lit, ref, to_output};

data_frame()
    ->read(from_csv(__DIR__ . '/output/date=*/{department}.csv'))
    ->filterPartitions(ref('department')->equals(lit('sales')))
    ->write(to_output())
    ->run();
```

A placeholder matches any file in that location and the partition value is taken from the file name
as-is. Appended files get randomized suffixes (`sales_a1b2c3.csv`), which become part of the value.

## Choosing Partition Columns

Aim for balanced partition sizes: `date` or `department` over a unique `id`, which would create one
tiny partition per row.

## Save Modes with Partitioning

The save mode decides how existing partition directories are handled.

### Overwrite

```php
<?php

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite, partition_by, ref};

data_frame()
    ->read(from_array([
        ['date' => '2024-01-01', 'amount' => 100],
        ['date' => '2024-01-02', 'amount' => 200],
    ]))
    ->write(to_csv(__DIR__ . '/output/sales.csv')
        ->partitionBy(partition_by(ref('date')))
        ->saveMode(overwrite()))
    ->run();
```

Removes all files in the partition directories being written to; partitions not in the current
dataset are preserved.

**Pitfall:** two separate writes to the same partition with `overwrite()` - the second deletes the
first. Use `append()`, or merge before writing.

### Append

```php
<?php

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{append, data_frame, from_array, partition_by, ref};

data_frame()
    ->read(from_array([['date' => '2024-01-01', 'amount' => 100]]))
    ->write(to_csv(__DIR__ . '/output/sales.csv')
        ->partitionBy(partition_by(ref('date')))
        ->saveMode(append()))
    ->run();
```

Adds files with randomized suffixes; repeated runs accumulate files.

### Ignore

```php
<?php

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{data_frame, from_array, ignore, partition_by, ref};

data_frame()
    ->read(from_array([['date' => '2024-01-01', 'amount' => 100]]))
    ->write(to_csv(__DIR__ . '/output/sales.csv')
        ->partitionBy(partition_by(ref('date')))
        ->saveMode(ignore()))
    ->run();
```

Skips the write if the partition directory exists. No error.

### Exception If Exists (Default)

```php
<?php

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{data_frame, from_array, partition_by, ref};

data_frame()
    ->read(from_array([['date' => '2024-01-01', 'amount' => 100]]))
    ->write(to_csv(__DIR__ . '/output/sales.csv')
        ->partitionBy(partition_by(ref('date'))))
    ->run();
```

Throws if any partition path already exists.

## Reading Partitioned Data

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, to_output};

data_frame()
    ->read(from_csv(__DIR__ . '/output/date=*/*.csv'))
    ->write(to_output())
    ->run();
```

Partition values from the path become regular columns. Given a schema that does not declare a
partition column, it is appended as a **string** column - declare it (`int_schema('date')`) to read
it as another type.

### Partition Pruning

`filterPartitions()` evaluates partition metadata and skips whole directories; `filter()` reads
everything and then discards.

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, lit, ref, to_output};

data_frame()
    ->read(from_csv(__DIR__ . '/output/date=*/department=*/*.csv'))
    ->filterPartitions(ref('date')->greaterThanEqual(lit('2024-01-01')))
    ->write(to_output())
    ->run();
```

### Path Partitions

`from_path_partitions()` reads partition metadata without opening the files.

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_path_partitions, to_output};

data_frame()
    ->read(from_path_partitions(__DIR__ . '/output/date=*/department=*/*.csv'))
    ->write(to_output())
    ->run();
```

Output carries `path` and `partitions` columns.

## Repartitioning

`repartition()` shuffles rows between batches in memory. It does not write directories - use the
loader's `partitionBy()` for that.
