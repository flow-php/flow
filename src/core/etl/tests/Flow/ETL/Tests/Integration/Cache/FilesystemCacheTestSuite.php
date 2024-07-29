<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\FilesystemCache;
use Flow\Filesystem\Path;

final class FilesystemCacheTestSuite extends CacheBaseTestSuite
{
    protected function cache() : Cache
    {
        return new FilesystemCache(
            $this->fs(),
            $this->serializer(),
            new Path(__DIR__ . '/var/filesystem-cache')
        );
    }
}
