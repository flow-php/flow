# Map

[DOC_LINK:/documentation/components/core/core.md]

Quick `Row` transformations are available through `DataFrame::map` function

```php 
<?php 

data_frame()
    ->read(from_array(...))
    ->map(
        schema(int_schema('id'), bool_schema('odd')),
        fn (Row $row) => row([...$row->values(), 'odd' => $row->get('id') % 2 === 0]),
    )
    ->write($loader)
    ->run();
```

`map` takes the output `Schema` first: a `Row` carries values only, so the shape of what the callback
produces has to be declared, not inferred.

It's the easiest but also the least performant way to transform data. Unless there is no better way
please try to avoid using map as it can't be automatically optimized by the engine.
