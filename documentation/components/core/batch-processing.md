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
    ->with($expensiveTransformation)
    ->write(to_output())
    ->run();
```

> **Performance Tip**: Optimal batch size depends on your data and available memory. Larger batches reduce I/O
> operations but increase memory usage. Start with 1000-5000 rows and adjust based on your specific use case.

### Source, re-slice, pipeline - three batch sizes

`withBatchSize()` bounds what a source **builds**; `batches()` re-slices what a source **already emitted** and
cannot lower its peak; `batchSize()` re-batches the pipeline **after** the source.

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{batches, data_frame, from_all, to_output};

data_frame()
    ->read(from_all(
        from_csv('orders.csv')->withBatchSize(500), // the source builds batches of at most 500 rows (default 100)
        batches(from_json('orders.json'), 50),      // re-sliced after the source built its own batches
    ))
    ->batchSize(1000)                               // re-batched after reading; the tip above is about this one
    ->write(to_output())
    ->run();
```

Database sources that page over the network default to `withBatchSize(1000)`: there, one batch is one round trip.

### batchBy() - Group related records together

```php
<?php

use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\{data_frame, from_array};

$dataFrame = data_frame()
    ->read(from_array($orders_with_line_items))
    ->batchBy('order_id', minSize: 1000) // Keep order line items together
    ->write(to_dbal_table_insert($connection, 'orders_table'))
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
    ->with($transformation)
    ->write($loader)
    ->run(analyze: analyze());

echo "Peak memory usage: " . $report->statistics()->memory->max()->inMb() . " bytes\n";
```