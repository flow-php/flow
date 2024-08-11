Read data from a json file.

```php
function from_parquet(
    string|Path|array $uri,
    array $columns = [],
    Options $options = new Options(),
    ByteOrder $byte_order = ByteOrder::LITTLE_ENDIAN,
    ?int $offset = null,
);
```

* `columns` - default [], list of columns to read, when empty, all columns will be read
* `options` - custom Parquet Reader [Options](https://github.com/flow-php/flow/blob/1.x/src/lib/parquet/src/Flow/Parquet/Options.php)
* `byte_order` - default `ByteOrder::LITTLE_ENDIAN`, the byte order of the parquet file
* `offset` - default null, rows to skip from the beginning of the file 