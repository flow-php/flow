# Select / Drop

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

## Select

Keep only the listed columns with `DataFrame::select()`:

```php 
<?php 

data_frame()
    ->read(from_array([['id' => 1, 'name' => 'Norbert', 'email' => 'norbert@example.com']]))
    ->select("id", "name")
    ->write(to_output())
    ->run();
```

## Drop

Remove the listed columns with `DataFrame::drop()`:

```php 
<?php 

data_frame()
    ->read(from_array([['id' => 1, 'name' => 'Norbert', '_tags' => 'internal']]))
    ->drop("_tags")
    ->write(to_output())
    ->run();
```