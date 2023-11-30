# Sort

- [⬅️️ Back](core.md)

Sorting is a common operation in data processing.
Thanks to implementation of External Sort algorithm, sorting as everything else is by default memory-safe. 
This means that even sorting 10gb file if doable in just a few megabytes of RAM.

> [!WARNING]
> Please remember that sort is an expensive operation, usually datasets are either
> loaded into destination storages, or reduced by filtering/grouping.
> Sorting needs to go through entire dataset and sort all Rows regardless of how
> big the dataset is compared to available memory. To achieve that, External Sort is using cache which relays on I/O that might become a bottleneck.

## Example

```php
<?php 

data_frame()
    ->read(from_sequence_number('id', 1, 10))
    ->sortBy(ref('id')->desc())
    ->collect()
    ->write(to_output(false))
    ->run()
```

Output:

```console
+----+
| id |
+----+
| 10 |
|  9 |
|  8 |
|  7 |
|  6 |
|  5 |
|  4 |
|  3 |
|  2 |
|  1 |
+----+
10 rows
```