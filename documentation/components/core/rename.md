# Rename

- [⬅️️ Back](/documentation/components/core/core.md)

DataFrame provides several methods for renaming entries (columns) in your data. These operations are lazy and don't execute until a trigger operation is called.

## Single Column Rename

To rename a single entry, use `DataFrame::rename()`:

```php 
<?php 

use function Flow\ETL\DSL\{data_frame, from_array, to_output};

data_frame()
    ->read(from_array([
        ['old_name' => 'value1', 'other' => 'data1'],
        ['old_name' => 'value2', 'other' => 'data2'],
    ]))
    ->rename('old_name', 'new_name')
    ->write(to_output())
    ->run();
```

## Batch Renaming

The `renameEach()` method allows you to rename multiple columns at once using various strategies:

### Available Strategies

- **`rename_style()`** - Changes entry names using string style conventions (camelCase, snake_case, etc.)
- **`rename_replace()`** - Replaces parts of entry names using search and replace patterns
