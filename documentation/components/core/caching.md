# Caching

[DOC_LINK:/documentation/components/core/core.md]

## Cache

The goal of cache is to serialize and save on disk (or in another location defined by Cache implementation)
already transformed dataset.

Cache runs the pipeline and saves each batch (`Rows`) into the cache, from where it can be read later.

Each batch is stored under its own cache key, and the pipeline's cache id points at an index - a batch with `key`
and `rows` columns listing those chunk keys and their row counts in insertion order.

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
    ->withEntry('name', ref('name')->upper())
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

By default, Flow is using Filesystem Cache, location of the cache storage can be adjusted through
the `FLOW_LOCAL_FILESYSTEM_CACHE_DIR` environment variable.

To only move the cache directory, set it on the config - the default cache keeps the pipeline's serializer
and backend:

```php
<?php

use function Flow\ETL\DSL\config_builder;

config_builder()->cacheDir(__DIR__ . '/var/cache');
```

To use different cache implementation please use `ConfigBuilder`

```php
<?php

use Flow\ETL\Cache\Implementation\PSRSimpleCache;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\Floe\FloeSerializer;
use Symfony\Component\Cache\Adapter\ArrayAdapter;
use Symfony\Component\Cache\Psr16Cache;

use function Flow\ETL\DSL\config_builder;

config_builder()
    ->cache(
        new PSRSimpleCache(
            new Psr16Cache(new ArrayAdapter()),
            new FloeSerializer(new AdaptiveBackend()),
        )
    );
```

The following implementations are available out of the box:

* [InMemory](/src/core/etl/src/Flow/ETL/Cache/Implementation/InMemoryCache.php)
* [LocalFilesystem](/src/core/etl/src/Flow/ETL/Cache/Implementation/FilesystemCache.php)
* [PSRSimpleCache](/src/core/etl/src/Flow/ETL/Cache/Implementation/PSRSimpleCache.php)
* [ApcuCache](/src/core/etl/src/Flow/ETL/Cache/Implementation/ApcuCache.php) - `new ApcuCache($serializer, $namespace)`,
  requires `ext-apcu`

PSRSimpleCache makes possible to use any of
the [psr/simple-cache-implementation](https://packagist.org/providers/psr/simple-cache-implementation)
but it does not come with any out of the box.

## Serialization

Persisting caches (`FilesystemCache`, `PSRSimpleCache`, `ApcuCache`) turn `Rows` into bytes through a
[`Serializer`](/src/core/etl/src/Flow/Serializer/Serializer.php). The default is
[`FloeSerializer`](/src/core/etl/src/Flow/Floe/FloeSerializer.php), which writes each batch as one
[Floe](/documentation/components/core/floe.md) BATCH frame and reads it back into the
[column backend](/documentation/components/core/column-backend.md) it was built with -
`new FloeSerializer($backend, $batchSize = 1000)`.

`Cache::get()` returns one fully materialized `Rows` per key. The caching processor writes exactly
one batch per cache key, so each key holds a single batch bounded by `DataFrame::cache(cacheBatchSize:)`
(falling back to `DataFrame::batchSize()`):

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array};

data_frame()
    ->read(from_array($data))
    ->cache('my-dataset', cacheBatchSize: 1000)
    ->run();
```

A `Serializer` writes to and reads from streams, so the default `FilesystemCache` streams the payload
directly to and from the cache file. `PSRSimpleCache` materializes the payload string - its PSR-16
backend stores string values - bounded by `cacheBatchSize`.

The default `FilesystemCache` shares the serializer of the rest of the pipeline (so it decodes into the configured
backend). Build one with `filesystem_cache()` only for a custom serializer or filesystem - a cache you build takes
its serializer from you:

```php
<?php

use function Flow\ETL\DSL\filesystem_cache;

filesystem_cache(new MyCustomSerializer());
```

`FilesystemCache` stores each entry in a file named after the cache key, with no extension, under hash-prefixed
subdirectories, and its schema the same way under `<key>.schema` - the entry bytes are whatever the configured serializer
produced.