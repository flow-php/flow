# Batch Processing

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Batch processing controls how data flows through the DataFrame pipeline. It changes the memory usage and the performance.

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

> Performance Tip: The best batch size depends on your data and the available memory. Larger batches reduce I/O
> operations, but they use more memory. Start with 1000-5000 rows, and change the value for your use case.

### Source and pipeline - two batch sizes

`withBatchSize()` limits the batches that a source builds. `batchSize()` re-batches the pipeline after the source.

```php
<?php

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\JSON\from_json;
use function Flow\ETL\DSL\{data_frame, from_all, to_output};

data_frame()
    ->read(from_all(
        from_csv('orders.csv')->withBatchSize(500),  // the source builds batches of at most 500 rows (default 100)
        from_json('orders.json')->withBatchSize(50), // the source builds batches of at most 50 rows
    ))
    ->batchSize(1000)                                // re-batched after reading; the tip above is about this one
    ->write(to_output())
    ->run();
```

Database sources that page over the network default to `withBatchSize(1000)`. For them, one batch is one round trip.

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

The `batchBy()` method makes sure that all rows with the same column value stay in the same batch. This is
important for:
- Hierarchical data (orders with line items, parent-child relationships)
- DELETE+INSERT operations (upsert patterns)
- Referential integrity during batch processing

Key behaviors:
- Groups are never split across batches. This keeps the data integrity.
- With `minSize`: batches grow up to the minimum size, and then yield at a group boundary.
- Without `minSize`: each unique group value gets its own batch.
- Batches can be larger than `minSize`, to keep large groups complete.
- Group values compare by value. Equal datetimes or uuids are one group. Nulls form one group.

> Use Case: you load orders with line items, with a DELETE+INSERT pattern. `batchBy('order_id')` makes sure that all line items of an order are in the same batch. This prevents foreign key violations.

## Data Collection

### collect() - Load all data into memory

```php
<?php

$dataFrame = data_frame()
    ->read($extractor)
    ->filter($condition)
    ->collect() // Collect all filtered data into single batch
    ->write($loader)
    ->run();
```

> Memory Warning: The `collect()` method loads all data into memory at the same time. With large datasets, the
> memory can run out. Use it only in these cases:
> - The full dataset surely fits in the available memory.
> - The next step needs the full dataset in one batch
> - You work with small to medium datasets

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

echo "Peak heap: " . $report->statistics()->heap->max()->inMb() . " MB\n";
echo "Process peak: " . $report->statistics()->processes->parent->inMb() . " MB\n";
```