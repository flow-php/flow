<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use function Flow\Filesystem\DSL\path;
use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\FilesystemCache;

final class FilesystemCacheTestSuite extends CacheBaseTestSuite
{
    protected function cache() : Cache
    {
        return new FilesystemCache(
            $this->fs(),
            $this->serializer(),
            path(__DIR__ . '/var/filesystem-cache')
        );
    }
}
