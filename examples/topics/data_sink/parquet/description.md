Write data to a parquet file.

```php
function to_parquet(string|Path $path) : Loader
```

Additional options:

* `withOptions(\Flow\Parquet\Options $options)` - [Options](https://github.com/flow-php/flow/blob/1.x/src/lib/parquet/src/Flow/Parquet/Options.php) for the parquet writer
* `withCompression(Compressions $compression)` - default `Compressions::SNAPPY`, supports one of the following [Compressions](https://github.com/flow-php/flow/blob/1.x/src/lib/parquet/src/Flow/Parquet/ParquetFile/Compressions.php)
* `withSchema(Schema $schema)` - when provided writer will use this instead of inferring schema from each rows batch