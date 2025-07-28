# Offset

- [⬅️️ Back](/documentation/components/core/core.md)

The offset operation skips a specified number of rows from the beginning of the dataset, commonly used for pagination
and data sampling.

## Basic Offset Usage

### offset() - Skip rows from beginning

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, to_output};

$dataFrame = data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Alice'],
        ['id' => 2, 'name' => 'Bob'],
        ['id' => 3, 'name' => 'Charlie'],
        ['id' => 4, 'name' => 'David'],
        ['id' => 5, 'name' => 'Eve'],
    ]))
    ->offset(2) // Skip first 2 rows
    ->write(to_output())
    ->run();

// Output: Charlie, David, Eve
```

## Performance Considerations

> **⚠️ Performance Warning**: The `offset()` method must iterate through and process all skipped rows to reach the
> offset position. For large offsets (e.g., `offset(1000000)`), this can significantly impact performance as the DataFrame
> still needs to read and process all data up to the offset point.
