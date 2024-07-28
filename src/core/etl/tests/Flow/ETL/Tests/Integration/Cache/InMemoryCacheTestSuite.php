<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache\Cache;
use Flow\ETL\Cache\Implementation\InMemoryCache;

final class InMemoryCacheTestSuite extends CacheBaseTestSuite
{
    protected function cache() : Cache
    {
        return new InMemoryCache();
    }
}
