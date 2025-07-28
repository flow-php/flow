# Data Retrieval

- [⬅️️ Back](/documentation/components/core/core.md)

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

> **⚠️ Memory Warning**: The `fetch()` method loads all requested rows into memory at once. Without a limit parameter,
> it will attempt to load the entire dataset into memory, which can cause memory exhaustion. Always use with a reasonable
> limit or prefer generator-based methods.

### count() - Count total rows

```php
<?php

$totalCount = $dataFrame->count();
echo "Total rows: $totalCount\n";
```

> **⚠️ Performance Warning**: The `count()` method must process the entire dataset to return the total count, which can
> be expensive for large datasets. Consider whether you actually need the exact count or if an approximation would
> suffice.

## Iteration with Callback

### forEach() - Process with callback

```php
<?php

$dataFrame->forEach(function (Rows $rows) {
    echo "Processing batch of " . $rows->count() . " rows\n";
    // Custom processing logic
});
```