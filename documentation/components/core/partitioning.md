# Partitioning

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Partitioning splits written data into `column=value` directories. A reader can then skip the directories that it
does not need.

You declare partitioning on the loader, not on the DataFrame.

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

A `{column}` placeholder in the destination path puts that partition into the file name. It does not make a
directory for it.

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

- Every placeholder must match a partition column. If it does not, the write fails.
- A path with placeholders must get partitioned rows.
- Placeholders can be in any segment of the path, and you can combine them: `/output/{date}/{department}_report.csv`.

### Reading Data Partitioned with Placeholders

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, lit, ref, to_output};

data_frame()
    ->read(from_csv(__DIR__ . '/output/date=*/{department}.csv'))
    ->filter(ref('department')->equals(lit('sales')))
    ->write(to_output())
    ->run();
```

A placeholder matches any file in that location. The partition value is the file name, without changes. Appended
files have the run in their name (`sales-a1b2c3d4e5f6a7b8.csv`). The run then becomes part of the value.

## The {part} placeholder

`{part}` in a destination is not a partition placeholder. Flow replaces it with the part number
(`part-00003.parquet`). The name is reserved. If a partition column named `part` goes to a `{part}` destination,
building the plan throws. Rename the column.

## Choosing Partition Columns

Use columns that give partitions of similar size, for example `date` or `department`. Do not use a unique `id`.
It makes one very small partition for each row.

## Partition Values

Flow writes each value in full. A typed read gets the value back without changes:

| Column type | Value                            | Directory                                       |
|-------------|----------------------------------|-------------------------------------------------|
| `datetime`  | `2026-10-02 13:00:00 UTC`        | `date=2026-10-02T13%3A00%3A00%2B00%3A00`        |
| `datetime`  | `2026-10-02 13:00:00.123456 UTC` | `date=2026-10-02T13%3A00%3A00.123456%2B00%3A00` |
| `date`      | `2026-10-02`                     | `date=2026-10-02`                               |
| `float`     | `1.0E-9`                         | `x=1.0e-9`                                      |

A `datetime` column makes one directory for each distinct value. For one directory for each day, partition by a `date`:

```php
<?php

use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite, partition_by, ref};
use function Flow\Types\DSL\type_date;

data_frame()
    ->read(from_array([
        ['created_at' => new DateTimeImmutable('2024-01-01 09:00:00'), 'amount' => 100],
        ['created_at' => new DateTimeImmutable('2024-01-01 13:00:00'), 'amount' => 200],
    ]))
    ->withColumn('day', ref('created_at')->cast(type_date()))
    ->write(to_parquet(__DIR__ . '/output/events.parquet')
        ->partitionBy(partition_by('day'))
        ->saveMode(overwrite()))
    ->run();
```

```text
output/
└── day=2024-01-01/
    └── events.parquet
```

## Save Modes with Partitioning

The save mode controls what happens to partition directories that exist.

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

Removes all files in the partition directories that the write goes to. Partitions that are not in the current
dataset stay.

Do not write to the same partition two times with `overwrite()`. The second write deletes the first. Use `append()`,
or merge the data before the write.

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

Adds files named after the run (`sales-<run>.csv`). Each run adds more files.

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

If the partition directory exists, the write is skipped. There is no error.

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

If a partition path exists, the write throws.

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

Partition values from the path become regular columns. If the schema does not declare a partition column, Flow
appends it as a string column. To read it as another type, declare it (`int_schema('date')`). A file can also hold
the partition column (`partition_by(...)->writeColumns()`). Then the file schema (Parquet, Floe) gives the type. A
`filter(ref('date')->between(lit($from), lit($to)))` with `DateTimeImmutable` bounds then prunes. Pruning skips the
files that cannot match. Flow casts string bounds (`lit('2026-10-02')`) to the `date` / `datetime` type of the
column, so they prune too.

### Partition Pruning

The optimizer moves a `filter()` that reads only partition columns into the file source. The source then skips
full directories. Every format does this. A partition filter after another filter also skips directories. Parquet
also takes a filter on a body column ([filter pushdown](/documentation/components/adapters/parquet.md#filter-pushdown)) and skips row groups with it.
For other formats, a filter on a body column stays a `Filter` step.
If a filter moved into the source, a `limit()` ([filter pushdown](/documentation/components/adapters/parquet.md#filter-pushdown)) after it goes to the source too.
Flow still prunes a partition column that you rename with `rename()` or copy with `withColumn('day', ref('date'))`.
Any other new definition of the column stops the push. Some filters can give a different answer each time they run.
Examples are `call()`, `now()`, `uuid()`, and `to_date_time()` with a format that leaves fields out (`'Y-m-d'`).
These filters still remove rows, but they prune nothing.

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, lit, ref, to_output};

data_frame()
    ->read(from_csv(__DIR__ . '/output/date=*/department=*/*.csv'))
    ->filter(ref('date')->greaterThanEqual(lit('2024-01-01')))
    ->limit(10)
    ->write(to_output())
    ->run();
```

`explain()` shows the test for the path of each file under `Files:`. It shows the test for each row under `Filter:`:

```text
#3 Write
│  Loader: StreamLoader
└─ #2 Limit
   │  Limit: 10
   └─ #1 Read
         Extractor: CSVExtractor
         Source: file:///output/date=*/department=*/*.csv
         Limit: 10
         Files: date >= '2024-01-01'
         Filter: date >= '2024-01-01'
```

### Path Partitions

`from_path_partitions()` reads the partition metadata. It does not open the files.

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_path_partitions, to_output};

data_frame()
    ->read(from_path_partitions(__DIR__ . '/output/date=*/department=*/*.csv'))
    ->write(to_output())
    ->run();
```

The output has `path`, a `partitions` map, and one string column for each partition (`date`, `department`). A
`filter()` on a partition column prunes the listing, as with any other file source.

## Repartitioning

`repartition()` moves rows in the stream so that all rows with the same values in the given columns are in one
batch. While the process stays under the memory limit, it groups the rows in memory. Past the limit, it first
partitions the input into buckets. You configure this with
`config_builder()->repartition(hash_repartition()->memoryLimit(...))`. It does not write directories. For
directories, use `partitionBy()` on the loader.
