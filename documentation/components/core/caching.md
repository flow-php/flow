# Caching

[DOC_LINK:/documentation/components/core/core.md]

## Cache

The goal of cache is to serialize and save on disk (or in another location defined by Cache implementation)
already transformed dataset.

Cache will run a pipeline, catching each Rows and saving them into cache
from where those rows can be later extracted.

Internally, each cached batch of Rows is stored under its own cache key, and the pipeline's cache id
points at an index - a single-column (`key`) Rows value listing those chunk keys in insertion order.

This is useful for operations that require full transformation of dataset before
moving forward, like, for example, sorting.

Another interesting use case for caching would be to share the dataset between multiple data processing pipelines.
So instead of going to datasource multiple times and then repeating all transformations, only one ETL would
do the whole job and others could benefit from the final form of dataset in a memory-safe way.

```php
<?php 

data_frame
    ->read(from_())
    ->withEntry('...', ref('...')->doSomething())
    ->cache()
    ->write(to_())
    ->run();
```

By default, Flow is using Filesystem Cache, location of the cache storage can be adjusted through
the `FLOW_LOCAL_FILESYSTEM_CACHE_DIR` environment variable.

To use different cache implementation please use `ConfigBuilder`

```php
<?php

use function Flow\ETL\DSL\config_builder;

config_builder()
  ->cache(
    new PSRSimpleCache(
        new Psr16Cache(
            new ArrayAdapter()
        )
    )
  );
```

The following implementations are available out of the box:

* [InMemory](/src/core/etl/src/Flow/ETL/Cache/Implementation/InMemoryCache.php)
* [LocalFilesystem](/src/core/etl/src/Flow/ETL/Cache/Implementation/FilesystemCache.php)
* [PSRSimpleCache](/src/core/etl/src/Flow/ETL/Cache/Implementation/PSRSimpleCache.php)

PSRSimpleCache makes possible to use any of
the [psr/simple-cache-implementation](https://packagist.org/providers/psr/simple-cache-implementation)
but it does not come with any out of the box.

## Serialization

Cache entries are stored as [Floe](/documentation/components/core/floe.md) files, streamed in both
directions: writes go straight to the cache file batch by batch (the payload string is never
materialized), reads decode one batch at a time. The batch size (default 1000 rows) bounds how many
rows cross the engine at once:

```php
<?php

use function Flow\ETL\DSL\{config_builder, filesystem_cache};

// through the config builder (default FilesystemCache)
config_builder()
    ->cacheSerializerBatchSize(1000);

// or constructing the cache directly
filesystem_cache(serializer_batch_size: 1000);
```

`Cache::get()` always returns a fully materialized `Rows` — the raw payload bytes are never held
next to the decoded rows, but the decoded rows themselves are unbounded. To keep the decoded side
bounded too, use `Cache::read()`:

```php
<?php

foreach ($cache->read('my-dataset') as $rows) {
    // FilesystemCache: one batch of Rows at a time
    // InMemory/PSRSimpleCache: the whole value, yielded once
}
```