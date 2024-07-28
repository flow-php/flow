<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache\Cache;
use Flow\ETL\Cache\Implementation\PSRSimpleCache;
use Symfony\Component\Cache\Adapter\RedisAdapter;
use Symfony\Component\Cache\Psr16Cache;

final class PSRSimpleRedisCacheTestSuite extends CacheBaseTestSuite
{
    protected function cache() : Cache
    {
        return new PSRSimpleCache(new Psr16Cache(new RedisAdapter(
            RedisAdapter::createConnection(
                'redis://' . \getenv('REDIS_HOST') . ':' . \getenv('REDIS_PORT') . '/0',
                [
                    'retry_interval' => 2,
                    'timeout' => 5,
                ]
            ),
        )));
    }
}
