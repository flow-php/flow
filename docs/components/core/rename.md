# Rename

- [⬅️️ Back](core.md)

There are multiple ways to rename entries in the data frame.

## Rename

To quickly rename single entry use Rows `DataFrame::rename`

```php 
<?php 

data_frame()
    ->read(from_array(...))
    ->rename("old_name", "new_name")
    ->write($loader)
    ->run();
```

## Rename All

To quickly rename, all entries use Rows `DataFrame::renameAll`

```php
data_frame()
    ->read(from_array([
        ['e_id' => 1, 'e_name' => 2],
        ['e_id' => 2, 'e_name' => 3],
        ['e_id' => 3, 'e_name' => 4],
        ['e_id' => 4, 'e_name' => 5],
    ]))
    ->renameAll('e_', 'entry_')
    ->run();
    
// Output will be:
// [
//     ['entry_id' => 1, 'entry_name' => 2],
//     ['entry_id' => 2, 'entry_name' => 3],
//     ['entry_id' => 3, 'entry_name' => 4],
//     ['entry_id' => 4, 'entry_name' => 5],
// ]    
```

## Remaining rename methods

- `DataFrame::renameAllLowerCase()`
- `DataFrame::renameAllStyle(StringStyles|string $style)`
- `DataFrame::renameAllUpperCase()`
- `DataFrame::renameAllUpperCaseFirst()`
- `DataFrame::renameAllUpperCaseWord()`

