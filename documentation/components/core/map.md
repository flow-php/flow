# Map

- [⬅️️ Back](core.md)

Quick `Row` transformations are available through `DataFrame::map` function

```php 
<?php 

data_frame()
    ->read(from_array(...))
    ->map(fn (Row $row) => $row->add(bool_entry('odd', $row->valueOf('id') % 2 === 0)))
    ->write($loader)
    ->run();
```

It's the easiest but also the least performant way to transform data. Unless there is no better way
please try to avoid using map as it can't be automatically optimized by the engine.
