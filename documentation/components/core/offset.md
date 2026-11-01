# Offset

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

The offset operation skips a specified number of rows at the start of the dataset. Common uses are pagination
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

> Performance Warning: The `offset()` method must go through and process all skipped rows to reach the offset
> position. Large offsets, for example `offset(1000000)`, can therefore be slow. The DataFrame still reads and
> processes all data up to the offset point.
