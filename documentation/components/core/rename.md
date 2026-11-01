# Rename

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

DataFrame has several methods to rename columns. These operations are lazy. Only a trigger operation executes them.

## Single Column Rename

To rename a single column, use `DataFrame::rename()`:

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

The `renameEach()` method renames multiple columns at the same time, with different strategies:

### Available Strategies

- `rename_style()`: changes column names with string style conventions, for example camelCase or snake_case
- `rename_replace()`: replaces parts of column names with search and replace patterns
