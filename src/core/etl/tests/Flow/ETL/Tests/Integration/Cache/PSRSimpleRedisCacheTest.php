<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Exception;
use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\PSRSimpleCache;
use Override;
use Symfony\Component\Cache\Adapter\RedisAdapter;
use Symfony\Component\Cache\Psr16Cache;

use function getenv;

final class PSRSimpleRedisCacheTest extends CacheTestCase
{
    private bool $redisAvailable = false;

    #[Override]
    protected function setUp(): void
    {
        try {
            RedisAdapter::createConnection($this->dsn(), ['retry_interval' => 1, 'timeout' => 1]);
            $this->redisAvailable = true;
        } catch (Exception) {
            self::markTestSkipped('Redis server is not available.');
        }

        parent::setUp();
    }

    #[Override]
    protected function tearDown(): void
    {
        // Skipped in setUp before the parent state was initialized - there is
        // nothing to clean up and the parent tearDown would error.
        if (!$this->redisAvailable) {
            return;
        }

        parent::tearDown();
    }

    protected function cache(): Cache
    {
        return new PSRSimpleCache(new Psr16Cache(new RedisAdapter(RedisAdapter::createConnection($this->dsn(), [
            'retry_interval' => 2,
            'timeout' => 5,
        ]))));
    }

    protected function dsn(): string
    {
        $host = getenv('REDIS_HOST');
        $port = getenv('REDIS_PORT');

        return 'redis://' . ($host === false ? 'localhost' : $host) . ':' . ($port === false ? '6379' : $port) . '/0';
    }
}
