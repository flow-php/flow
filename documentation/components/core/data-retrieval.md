# Data Retrieval

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

DataFrame provides several methods for retrieving processed data. These methods are trigger operations that execute the
entire pipeline.

## Memory-Safe Retrieval (Recommended)

These methods use generators to maintain constant memory usage regardless of dataset size:

### get() - Retrieve as Rows batches

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array};

$dataFrame = data_frame()->read(from_array($largeDataset));

foreach ($dataFrame->get() as $rows) {
    echo "Processing batch of " . $rows->count() . " rows\n";
    // Process each batch
    foreach ($rows as $row) {
        // Process individual row
    }
}
```

When `$largeDataset` is not an array - a generator, or any other one-shot iterable - `from_array()` reads
it once and writes every row to a temporary file before the first batch is produced, so that the schema it
reports is exact. Memory stays constant, but the dataset is on disk for as long as the extractor lives -
which is what lets you call `get()` or `run()` on it more than once, exactly as you can with an
array. (`schema()` never consumed the source in the first place - it answers from the plan.) Declare the schema up front with `from_array($largeDataset)->withSchema($schema)` to skip the spill
entirely; a declared schema streams the source directly and can therefore be read only once.

### getEach() - Retrieve individual Rows

```php
<?php

foreach ($dataFrame->getEach() as $row) {
    echo "ID: " . $row->get('id')->value() . "\n";
    echo "Name: " . $row->get('name')->value() . "\n";
}
```

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
foreach ($firstTen as $row) {
    // Process row
}

// Fetch all results (dangerous for large datasets!)
$allRows = $dataFrame->fetch(); // Can cause memory exhaustion
```

> **Memory Warning**: The `fetch()` method loads all requested rows into memory at once. Without a limit parameter,
> it will attempt to load the entire dataset into memory, which can cause memory exhaustion. Always use with a reasonable
> limit or prefer generator-based methods.

### count() - Count total rows

```php
<?php

$totalCount = $dataFrame->count();
echo "Total rows: $totalCount\n";
```

A frame that only reads one source is counted from the source's statistics when they are exact - a Parquet or Floe
file answers from its footer without reading a row (`explain(Trigger::count)` shows it):

```php
<?php

$totalCount = data_frame()->read(from_parquet('orders.parquet'))->count();
```

> **Performance Warning**: Any other frame - a filter, a limit, a new column, a join, an estimated source - is executed
> in full to count its rows, which can be expensive for large datasets.

## Iteration with Callback

### forEach() - Process with callback

```php
<?php

$dataFrame->forEach(function (Rows $rows) {
    echo "Processing batch of " . $rows->count() . " rows\n";
});
```

`forEach()` owns the streaming loop. `run(bool|Analyze $analyze = false)` takes no callback: it executes the frame's
sinks and returns a `Report` when asked to analyze.

```php
<?php

$dataFrame->write(to_json('out.json'))->run();

$report = $dataFrame->write(to_json('out.json'))->run(analyze: analyze()->withSchema());
```

`withSourceStatistics()` puts the rows every source declared next to the rows it actually yielded - the output row
count alone cannot tell them apart once a filter or a join sits in between:

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

`rowsError()` is null without an estimate, and for a read that did not cover the whole source - a pushed `limit()` or
partition filter, or a read stopped early - since the declaration describes the whole source:

```php
<?php

foreach ($report->sources() as $source) {
    $source->complete;                      // false after a pushed limit() or a stopped read
}
```