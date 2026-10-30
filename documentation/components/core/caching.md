# Caching

[DOC_LINK:/documentation/components/core/core.md]

## Cache

The goal of cache is to serialize and save on disk (or in another location defined by the cache storage)
already transformed dataset.

Cache runs the pipeline and appends each batch (`Rows`) under the cache id, from where it can be read later in the
same order.

Another interesting use case for caching would be to share the dataset between multiple data processing pipelines.
So instead of going to datasource multiple times and then repeating all transformations, only one ETL would
do the whole job and others could benefit from the final form of dataset in a memory-safe way.

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
the pipeline's backend. The location can be adjusted through the `FLOW_LOCAL_FILESYSTEM_CACHE_DIR` environment
variable, or on the config:

```php
<?php

use function Flow\ETL\DSL\config_builder;

config_builder()->cacheDir(__DIR__ . '/var/cache');
```

To use a different cache, pass a `RowsStorage` to `ConfigBuilder`:

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

The following implementations are available out of the box:

* [MemoryRowsStorage](/src/core/etl/src/Flow/ETL/Storage/MemoryRowsStorage.php)
* [FilesystemRowsStorage](/src/core/etl/src/Flow/ETL/Storage/FilesystemRowsStorage.php) - the default,
  `filesystem_cache($dir, $filesystem, $backend)`
* [PsrRowsStorage](/src/core/etl/src/Flow/ETL/Storage/PsrRowsStorage.php) - any
  [psr/simple-cache implementation](https://packagist.org/providers/psr/simple-cache-implementation)
* [ApcuRowsStorage](/src/core/etl/src/Flow/ETL/Storage/ApcuRowsStorage.php) - `new ApcuRowsStorage($serializer, $namespace)`,
  requires `ext-apcu`

## Batches and Serialization

`RowsStorage::get()` yields the cached batches one by one. Each one holds at most `DataFrame::cache(cacheBatchSize:)`
rows (falling back to `DataFrame::batchSize()`):

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array($data))
    ->cache('my-dataset', cacheBatchSize: 1000)
    ->run();
```

`FilesystemRowsStorage` writes one Floe file per cache id, `<dir>/flow-php-rows/<xxh128 of the id>/<id>.floe`, and
reads it back in the backend it was given. A file without a footer - a write cut short by a killed process - counts
as missing, so the next run writes it again.

`PsrRowsStorage` and `ApcuRowsStorage` store each batch as a string, made by the
[`Serializer`](/src/core/etl/src/Flow/ETL/Storage/Serializer/Serializer.php) they are given -
[`FloeSerializer`](/src/core/etl/src/Flow/ETL/Storage/Floe/FloeSerializer.php) writes it as one Floe BATCH frame
and reads it back into its [column backend](/documentation/components/core/column-backend.md).
