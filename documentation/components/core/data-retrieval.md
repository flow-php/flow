# Data Retrieval

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

DataFrame has several methods that return processed data. These methods are trigger operations that execute the
full pipeline.

## Memory-Safe Retrieval (Recommended)

These methods use generators, so the memory usage stays constant for each dataset size:

### get() - Retrieve as Rows batches

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array};

$dataFrame = data_frame()->read(from_array($largeDataset));

foreach ($dataFrame->get() as $rows) {
    echo "Processing batch of " . $rows->count() . " rows\n";
    // Process each batch
    foreach ($rows->toArray() as $row) {
        // Process individual row
    }
}
```

`$largeDataset` can be a value that is not an array, for example a generator or a different one-shot iterable.
`from_array()` then reads it once and writes each row to a temporary file before the first batch. The schema that it
reports is therefore exact. Memory stays constant, but the dataset stays on disk while the extractor lives. Because
of this, you can call `get()` or `run()` on it more than once, exactly as with an array. (`schema()` never used the
source in the first place. It answers from the plan.) To skip the spill, declare the schema first with
`from_array($largeDataset)->withSchema($schema)`. A declared schema streams the source directly, so you can read it
only once.

### getAsArray() - Retrieve as array batches

```php
<?php

foreach ($dataFrame->getAsArray() as $rowsArray) {
    // $rowsArray is an array of arrays
    foreach ($rowsArray as $rowArray) {
        echo "ID: " . $rowArray['id'] . "\n";
    }
}
```

### getEachAsArray() - Retrieve individual arrays

```php
<?php

foreach ($dataFrame->getEachAsArray() as $rowArray) {
    echo "ID: " . $rowArray['id'] . "\n";
    echo "Name: " . $rowArray['name'] . "\n";
}
```

### fetch() - Load into memory

```php
<?php

// Fetch limited results (safe)
$firstTen = $dataFrame->fetch(10);
foreach ($firstTen->toArray() as $row) {
    echo "ID: " . $row['id'] . "\n";
}

// Fetch all results (dangerous for large datasets!)
$allRows = $dataFrame->fetch(); // Can cause memory exhaustion
```

> Memory Warning: The `fetch()` method loads all requested rows into memory at the same time. Without a limit
> parameter, it tries to load the full dataset into memory, and the memory can run out. Always use it with a reasonable
> limit, or use generator-based methods.

### count() - Count total rows

```php
<?php

$totalCount = $dataFrame->count();
echo "Total rows: $totalCount\n";
```

A frame that only reads one source can use the exact statistics of the source for the count. A Parquet or Floe
file answers from its footer and reads no row. `explain(Trigger::count)` shows it:

```php
<?php

$totalCount = data_frame()->read(from_parquet('orders.parquet'))->count();
```

> Performance Warning: All other frames execute in full to count their rows. Examples are frames with a filter, a
> limit, a new column, a join or an estimated source. This can be expensive for large datasets.

## Iteration with Callback

### forEach() - Process with callback

```php
<?php

$dataFrame->forEach(function (Rows $rows) {
    echo "Processing batch of " . $rows->count() . " rows\n";
});
```

`forEach()` owns the streaming loop. `run(bool|Analyze $analyze = false)` takes no callback. It executes the writes
of the frame. With analyze, it also returns a `Report`.

```php
<?php

$dataFrame->write(to_json('out.json'))->run();

$report = $dataFrame->write(to_json('out.json'))->run(analyze: analyze()->withSchema());
```

`withSourceStatistics()` puts the rows that each source declared next to the rows that it really yielded. With a
filter or a join between them, the output row count alone cannot tell them apart:

```php
<?php

$report = data_frame()
    ->read(from_parquet('orders/*.parquet'))
    ->filter(ref('total')->greaterThan(lit(100)))
    ->run(analyze: analyze()->withSourceStatistics());

foreach ($report->sources() as $source) {
    echo $source->extractor;                // ParquetExtractor
    echo $source->declared->rows->estimate; // 20000 - extrapolated from the first footer
    echo $source->rows;                     // 29000 - read
    echo $source->rowsError();              // 0.31 - |declared - read| / read
}
```

`rowsError()` is null without an estimate. It is also null for a read that did not cover the full source, because
the declaration describes the full source. Examples are a pushed `limit()` or partition filter, or a read that stopped
early:

```php
<?php

foreach ($report->sources() as $source) {
    $source->complete;                      // false after a pushed limit() or a stopped read
}
```