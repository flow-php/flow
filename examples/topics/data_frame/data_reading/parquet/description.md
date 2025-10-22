Read data from a parquet file.

```php
function from_parquet(string|Path $uri);
```

Additional options:

* `withColumns(array $columns)` - default [], list of columns to read when not set, all columns will be read
* `withOptions(Options $options)` - custom Parquet Reader [Options](https://github.com/flow-php/flow/blob/1.x/src/lib/parquet/src/Flow/Parquet/Options.php)
* `withByteOrder(ByteOrder $order)` - default `ByteOrder::LITTLE_ENDIAN`, the byte order of the parquet file
* `withOffset(int $offset)` - default null, rows to skip from the beginning of the file 