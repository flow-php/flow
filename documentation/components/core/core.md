# Data Frame

- [⬅️️ Back](../../quick-start.md)

A Data Frame is a structured collection of tabular data, similar to a spreadsheet.  
It organizes information into rows and columns, making it easy to understand, filter, and transform.  
Using a Data Frame, you can quickly merge, clean, or modify data for your ETL processes,  
allowing developers to focus more on transformations rather than low-level data handling.

Unlike loading an entire dataset at once, a Data Frame processes information in smaller, manageable chunks.  
As it moves through the data, it only keeps a limited number of rows in memory at any given time.  
This approach helps avoid running out of memory, making it efficient and scalable for handling large datasets.

Simple example of reading from php array and writing to stdout.

```php
<?php

data_frame()
    ->read(from_array([
        ['id' => 1],
        ['id' => 2],
        ['id' => 3],
        ['id' => 4],
        ['id' => 5],
    ]))
    ->collect()
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
```