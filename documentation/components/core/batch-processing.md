# Batch Processing

- [⬅️️ Back](/documentation/components/core/core.md)

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

## Data Collection

### collect() - Load all data into memory

```php
<?php

$dataFrame = data_frame()
    ->read($extractor)
    ->filter($condition)
    ->collect() // Collect all filtered data into single batch
    ->sortBy(col('name')) // Now can sort the collected data
    ->write($loader)
    ->run();
```

> **⚠️ Memory Warning**: The `collect()` method loads all data into memory at once. This can cause memory exhaustion
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