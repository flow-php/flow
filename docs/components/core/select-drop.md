# Select / Drop

- [⬅️️ Back](core.md)

## Select

To quickly select only relevant entries use Rows `DataFrame::select`

```php 
<?php 

data_frame()
    ->read(from_array(...))
    ->select("id", "name")
    ->write(to_otuput())
    ->run();
```

## Drop

To quickly drop irrelevant entries use Rows `DataFrame::drop`

```php 
<?php 

data_frame()
    ->read(from_array(...))
    ->drop("_tags")
    ->write(to_otuput())
    ->run();
```