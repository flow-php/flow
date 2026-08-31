# Partitioning

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Partitioning divides data into logical groups based on column values, enabling more efficient processing of large datasets and reducing memory usage.

## Basic Partitioning

### partitionBy() - Partition by columns

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, col, to_parquet};

data_frame()
    ->read(from_array([
        ['date' => '2024-01-01', 'department' => 'sales', 'amount' => 100],
        ['date' => '2024-01-01', 'department' => 'marketing', 'amount' => 200],
        ['date' => '2024-01-02', 'department' => 'sales', 'amount' => 150],
        ['date' => '2024-01-02', 'department' => 'marketing', 'amount' => 250],
    ]))
    ->partitionBy('date') // Partition by date
    ->write(to_parquet(__DIR__ . '/output/sales.parquet'))
    ->run();
```

**File structure:**
```
output/
├── date=2024-01-01/
│   └── sales.parquet       # Contains 2 rows (sales + marketing for Jan 1)
└── date=2024-01-02/
    └── sales.parquet       # Contains 2 rows (sales + marketing for Jan 2)
```

Each partition creates a separate directory with the format `column=value`, and data files are written inside those directories.

## Multi-Column Partitioning

```php
<?php

data_frame()
    ->read(from_array([
        ['date' => '2024-01-01', 'department' => 'sales', 'amount' => 100],
        ['date' => '2024-01-01', 'department' => 'marketing', 'amount' => 200],
        ['date' => '2024-01-02', 'department' => 'sales', 'amount' => 150],
        ['date' => '2024-01-02', 'department' => 'marketing', 'amount' => 250],
    ]))
    ->partitionBy('date', 'department') // Partition by date AND department
    ->write(to_parquet(__DIR__ . '/output/sales.parquet'))
    ->run();
```

**File structure with nested partitions:**
```
output/
├── date=2024-01-01/
│   ├── department=sales/
│   │   └── sales.parquet       # 1 row: sales on Jan 1
│   └── department=marketing/
│       └── sales.parquet       # 1 row: marketing on Jan 1
└── date=2024-01-02/
    ├── department=sales/
    │   └── sales.parquet       # 1 row: sales on Jan 2
    └── department=marketing/
        └── sales.parquet       # 1 row: marketing on Jan 2
```

Each partition column creates an additional nesting level in the directory hierarchy.

### Dropping Partitions

```php
<?php

$dataFrame = data_frame()
    ->read($extractor)
    ->partitionBy('date')
    ->with($transformation)
    ->dropPartitions() // Remove partition information but keep data
    ->write($loader)
    ->run();

// Drop partitions AND partition columns
$dataFrame
    ->partitionBy('date')
    ->dropPartitions(dropPartitionColumns: true) // Also removes 'date' column
    ->run();
```

## Partition Placeholders

By default every partition becomes a `column=value` directory. With `{column}` placeholders in the destination path, selected partitions can become part of the file (or directory) name instead:

```php
<?php

data_frame()
    ->read(from_array([
        ['date' => '2024-01-01', 'department' => 'sales', 'amount' => 100],
        ['date' => '2024-01-01', 'department' => 'marketing', 'amount' => 200],
    ]))
    ->partitionBy('date', 'department')
    ->write(to_parquet(__DIR__ . '/output/{department}.parquet'))
    ->run();
```

**File structure:**
```
output/
├── date=2024-01-01/
│   ├── sales.parquet
│   └── marketing.parquet
```

Partitions consumed by placeholders are removed from the `column=value` directory chain; all remaining partitions still become directories. Placeholders can appear in any path segment and can be combined, for example `to_csv(__DIR__ . '/output/{date}/{department}_report.csv')`.

Rules:

- Every placeholder must match a `partitionBy()` column, otherwise the write fails.
- A destination path with placeholders requires partitioned rows - without `partitionBy()` the write fails.
- Save modes behave exactly like with directory partitions, applied to the resolved file path.

### Reading Data Partitioned with Placeholders

Placeholders work in extractor paths too - matching files like a wildcard and re-attaching the partition value from the file name:

```php
<?php

data_frame()
    ->read(from_parquet(__DIR__ . '/output/date=*/{department}.parquet'))
    ->filterPartitions(ref('department')->equals(lit('sales'))) // partition pruning works too
    ->write(to_output())
    ->run();
```

Keep in mind that a placeholder matches any file in that location, and the partition value is taken from the file name as-is. Files appended to an existing location get randomized suffixes (`sales_a1b2c3.parquet`), which become part of the recovered partition value.

## Performance Considerations

### Choosing Partition Columns

```php
<?php

// Good partitioning - balanced partition sizes
$dataFrame->partitionBy('date'); // Assuming data is spread across dates

// Bad partitioning - unbalanced partitions
$dataFrame->partitionBy('id'); // If IDs are unique, creates many tiny partitions

// Good partitioning - moderate cardinality
$dataFrame->partitionBy('department'); // Assuming reasonable number of departments
```

## Save Modes with Partitioning

When writing partitioned data, the save mode determines how existing partition directories are handled.

### Overwrite Mode

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, overwrite, ref};
use function Flow\ETL\Adapter\CSV\to_csv;

data_frame()
    ->read(from_array([
        ['date' => '2024-01-01', 'amount' => 100],
        ['date' => '2024-01-02', 'amount' => 200],
    ]))
    ->partitionBy(ref('date'))
    ->write(to_csv(__DIR__ . '/output/sales.csv')->saveMode(overwrite()))
    ->run();
```

**Behavior:**
- Removes ALL files within partition directories being written to
- Partitions NOT in the current dataset are preserved
- Running twice with the same partition values replaces the first write completely

**Common pitfall:** If you write two separate DataFrames to the same partitions using `overwrite()`, the second write deletes data from the first:

```php
<?php

// First write - creates date=2024-01-01/sales.csv with amount=100
data_frame()
    ->read(from_array([['date' => '2024-01-01', 'amount' => 100]]))
    ->partitionBy(ref('date'))
    ->write(to_csv(__DIR__ . '/output/sales.csv')->saveMode(overwrite()))
    ->run();

// Second write - DELETES the 100 and writes 200
data_frame()
    ->read(from_array([['date' => '2024-01-01', 'amount' => 200]]))
    ->partitionBy(ref('date'))
    ->write(to_csv(__DIR__ . '/output/sales.csv')->saveMode(overwrite()))
    ->run();

// Result: date=2024-01-01/sales.csv contains ONLY amount=200
```

To combine data from multiple sources into the same partition, use `append()` mode or merge data before writing.

### Append Mode

```php
<?php

data_frame()
    ->read(from_array([['date' => '2024-01-01', 'amount' => 100]]))
    ->partitionBy(ref('date'))
    ->write(to_csv(__DIR__ . '/output/sales.csv')->saveMode(append()))
    ->run();
```

**Behavior:**
- Creates new files with randomized suffixes in existing partition directories
- Does not remove existing files
- Multiple runs accumulate files (may cause duplicates)

### Ignore Mode

```php
<?php

data_frame()
    ->read(from_array([['date' => '2024-01-01', 'amount' => 100]]))
    ->partitionBy(ref('date'))
    ->write(to_csv(__DIR__ . '/output/sales.csv')->saveMode(ignore()))
    ->run();
```

**Behavior:**
- Skips writing if partition directory already exists
- No error thrown, silently continues

### Exception If Exists Mode (Default)

```php
<?php

data_frame()
    ->read(from_array([['date' => '2024-01-01', 'amount' => 100]]))
    ->partitionBy(ref('date'))
    ->write(to_csv(__DIR__ . '/output/sales.csv')) // Default mode
    ->run();
```

**Behavior:**
- Throws `RuntimeException` if any partition path already exists
- Safest option to prevent accidental overwrites

## Reading Partitioned Data

Read partitioned data using glob patterns to match partition directories:

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, to_output};

data_frame()
    ->read(from_csv(__DIR__ . '/output/date=*/*.csv'))
    ->write(to_output())
    ->run();
```

Partition values from the path become regular row columns. When the extractor is given a schema that
does not declare a partition column, the column is appended to the schema as a **string** column -
declare it explicitly (e.g. `int_schema('date')`) to read partition values as a different type.

### Partition Pruning

Skip entire partitions without reading their contents using `filterPartitions()`:

```php
<?php

data_frame()
    ->read(from_csv(__DIR__ . '/output/date=*/department=*/*.csv'))
    ->filterPartitions(ref('date')->greaterThanEqual(lit('2024-01-01')))
    ->write(to_output())
    ->run();
```

Unlike `filter()` which reads all data then discards non-matching rows, `filterPartitions()` evaluates partition metadata first and only reads matching partitions - significantly improving performance for large datasets.

### Path Partitions

Extract partition metadata without reading file contents using `from_path_partitions()`:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_path_partitions, to_output};

data_frame()
    ->read(from_path_partitions(__DIR__ . '/output/date=*/department=*/*.csv'))
    ->write(to_output())
    ->run();

// Output includes 'path' and 'partitions' columns
```

Useful for discovering available partitions or building file manifests before processing data.