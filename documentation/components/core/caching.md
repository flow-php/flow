# Caching

[DOC_LINK:/documentation/components/core/core.md]

## Cache

The cache serializes an already transformed dataset and saves it on disk, or in a different location of the cache
storage.

The cache runs the pipeline and appends each batch (`Rows`) under the cache id. A later read gets the batches in the
same order.

The cache can also share the dataset between multiple data processing pipelines. The pipelines then do not read the
data source many times and repeat all transformations. One ETL does the full job, and the others use the final
dataset in a memory-safe way.

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, int_schema, ref, schema, str_schema, to_output};

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'norbert'],
        ['id' => 2, 'name' => 'jane'],
        ['id' => 3, 'name' => 'john'],
    ], schema(int_schema('id'), str_schema('name'))))
    ->withColumn('name', ref('name')->upper())
    ->cache()
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
```

```text
+----+---------+
| id |    name |
+----+---------+
|  1 | NORBERT |
|  2 |    JANE |
|  3 |    JOHN |
+----+---------+
3 rows
```

By default, Flow writes the cache as [Floe](/documentation/components/core/floe.md) files on the local filesystem, in
the backend of the pipeline. To change the location, use the `FLOW_LOCAL_FILESYSTEM_CACHE_DIR` environment
variable, or the configuration:

```php
<?php

use function Flow\ETL\DSL\config_builder;

config_builder()->cacheDir(__DIR__ . '/var/cache');
```

For a different cache, give a `RowsStorage` to `ConfigBuilder`:

```php
<?php

use Flow\ETL\Storage\Floe\FloeSerializer;
use Flow\ETL\Storage\PsrRowsStorage;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Symfony\Component\Cache\Adapter\ArrayAdapter;
use Symfony\Component\Cache\Psr16Cache;

use function Flow\ETL\DSL\config_builder;

config_builder()
    ->cache(
        new PsrRowsStorage(
            new Psr16Cache(new ArrayAdapter()),
            new FloeSerializer(new AdaptiveBackend()),
        )
    );
```

These implementations are available by default:

* [MemoryRowsStorage](/src/core/etl/src/Flow/ETL/Storage/MemoryRowsStorage.php)
* [FilesystemRowsStorage](/src/core/etl/src/Flow/ETL/Storage/FilesystemRowsStorage.php): the default,
  `filesystem_cache($dir, $filesystem, $backend)`
* [PsrRowsStorage](/src/core/etl/src/Flow/ETL/Storage/PsrRowsStorage.php): each
  [psr/simple-cache implementation](https://packagist.org/providers/psr/simple-cache-implementation)
* [ApcuRowsStorage](/src/core/etl/src/Flow/ETL/Storage/ApcuRowsStorage.php): `new ApcuRowsStorage($serializer, $namespace)`,
  requires `ext-apcu`

## Batches and Serialization

`RowsStorage::get()` yields the cached batches one by one. Each one holds at most `DataFrame::cache(cacheBatchSize:)`
rows. Without that value, it uses `DataFrame::batchSize()`:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array($data))
    ->cache('my-dataset', cacheBatchSize: 1000)
    ->run();
```

`FilesystemRowsStorage` writes one Floe file for each cache id, `<dir>/flow-php-rows/<xxh128 of the id>/<id>.floe`.
It reads the file back in the backend that it got. A file without a footer, for example from a write that a killed
process stopped, counts as missing. The next run writes it again.

`PsrRowsStorage` and `ApcuRowsStorage` store each batch as a string. The
[`Serializer`](/src/core/etl/src/Flow/ETL/Storage/Serializer/Serializer.php) that they get makes the string.
[`FloeSerializer`](/src/core/etl/src/Flow/ETL/Storage/Floe/FloeSerializer.php) writes it as one Floe BATCH frame,
and reads it back into its [column backend](/documentation/components/core/column-backend.md).
