# Caching

- [⬅️️ Back](core.md)

## Cache

The goal of cache is to serialize and save on disk (or in another location defined by Cache implementation)
already transformed dataset.

Cache will run a pipeline, catching each Rows and saving them into cache
from where those rows can be later extracted.

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
environment variable `CACHE_DIR_ENV`.

To use different cache implementation please use `ConfigBuilder`

```php

Config::default()
  ->cache(
    new PSRSimpleCache(
        new Psr16Cache(
            new ArrayAdapter()
        ),
        new NativePHPSerializer()
    )
  );
```

The following implementations are available out of the box:

* [InMemory](../../../src/core/etl/src/Flow/ETL/Cache/InMemoryCache.php)
* [LocalFilesystem](../../../src/core/etl/src/Flow/ETL/Cache/LocalFilesystemCache.php)
* [Null](../../../src/core/etl/src/Flow/ETL/Cache/NullCache.php)
* [PSRSimpleCache](../../../src/core/etl/src/Flow/ETL/Cache/PSRSimpleCache.php)

PSRSimpleCache makes possible to use any of the [psr/simple-cache-implementation](https://packagist.org/providers/psr/simple-cache-implementation)
but it does not come with any out of the box.