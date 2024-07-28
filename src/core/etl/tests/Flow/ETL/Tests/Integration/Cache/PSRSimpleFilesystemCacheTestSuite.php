<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache\Cache;
use Flow\ETL\Cache\Implementation\PSRSimpleCache;
use Symfony\Component\Cache\Adapter\FilesystemAdapter;
use Symfony\Component\Cache\Psr16Cache;

final class PSRSimpleFilesystemCacheTestSuite extends CacheBaseTestSuite
{
    protected function cache() : Cache
    {
        return new PSRSimpleCache(new Psr16Cache(new FilesystemAdapter(directory: __DIR__ . '/var/psr-simple-file-cache')));
    }
}
