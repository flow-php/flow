<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\PSRSimpleCache;
use Symfony\Component\Cache\Adapter\RedisAdapter;
use Symfony\Component\Cache\Psr16Cache;

use function getenv;

final class PSRSimpleRedisCacheTestSuite extends CacheBaseTestSuite
{
    protected function cache(): Cache
    {
        $host = getenv('REDIS_HOST');
        $port = getenv('REDIS_PORT');

        return new PSRSimpleCache(new Psr16Cache(new RedisAdapter(RedisAdapter::createConnection(
            'redis://' . ($host === false ? 'localhost' : $host) . ':' . ($port === false ? '6379' : $port) . '/0',
            [
                'retry_interval' => 2,
                'timeout' => 5,
            ],
        ))));
    }
}
