# Batch Processing

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Batch processing controls how data flows through the DataFrame pipeline, affecting memory usage and performance.

## Batch Size Control

### batchSize() - Control processing chunks

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, to_output};

$dataFrame = data_frame()
    ->read(from_array($largeDataset))
    ->batchSize(1000) // Process in batches of 1000 rows
    ->map($expensiveTransformation)
    ->write(to_output())
    ->run();
```

> **Performance Tip**: Optimal batch size depends on your data and available memory. Larger batches reduce I/O
> operations but increase memory usage. Start with 1000-5000 rows and adjust based on your specific use case.

### batchBy() - Group related records together

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, to_database};

$dataFrame = data_frame()
    ->read(from_array($orders_with_line_items))
    ->batchBy('order_id', minSize: 1000) // Keep order line items together
    ->write(to_database($connection, 'orders_table'))
    ->run();
```

The `batchBy()` method ensures that all rows with the same column value stay in the same batch. This is critical when:
- Processing hierarchical data (orders with line items, parent-child relationships)
- Performing DELETE+INSERT operations (upsert patterns)
- Maintaining referential integrity during batch processing

**Key behaviors:**
- Groups are **never** split across batches (preserves data integrity)
- When `minSize` is specified: batches accumulate until reaching minimum size, then yield on group boundary
- When `minSize` is omitted: each unique group value gets its own batch
- Batches may exceed `minSize` to keep large groups intact

> **Use Case**: If you're loading orders with line items and using a DELETE+INSERT pattern, `batchBy('order_id')` ensures all line items for an order are in the same batch, preventing foreign key violations.

## Data Collection

### collect() - Load all data into memory

```php
<?php

$dataFrame = data_frame()
    ->read($extractor)
    ->filter($condition)
    ->collect() // Collect all filtered data into single batch
    ->sortBy([col('name')]) // Now can sort the collected data
    ->write($loader)
    ->run();
```

> **Memory Warning**: The `collect()` method loads all data into memory at once. This can cause memory exhaustion
> with large datasets. Use only when:
> - You're certain the entire dataset fits comfortably in available memory
> - You need operations that require all data (like sorting)
> - You're working with small to medium datasets

## Memory Management Strategies

## Monitoring Memory Usage

```php
<?php

use function Flow\ETL\DSL\analyze;

$report = data_frame()
    ->read($extractor)
    ->batchSize(1000)
    ->map($transformation)
    ->write($loader)
    ->run(analyze: analzyze());

echo "Peak memory usage: " . $report->statistics()->memory->max()->inMb() . " bytes\n";
```