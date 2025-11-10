# Partitioning

- [⬅️️ Back](/documentation/components/core/core.md)

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
    ->map($transformation)
    ->dropPartitions() // Remove partition information but keep data
    ->write($loader)
    ->run();

// Drop partitions AND partition columns
$dataFrame
    ->partitionBy('date')
    ->dropPartitions(dropPartitionColumns: true) // Also removes 'date' column
    ->run();
```

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