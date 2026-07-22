<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Bucketing\Storage;

use Exception;
use Flow\ETL\Bucketing\Storage\PSRCacheBuckets;
use Flow\ETL\Row;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Override;
use Symfony\Component\Cache\Adapter\RedisAdapter;
use Symfony\Component\Cache\Psr16Cache;

use function array_map;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function getenv;

final class PSRCacheBucketsTest extends FlowIntegrationTestCase
{
    #[Override]
    protected function setUp(): void
    {
        parent::setUp();

        $host = getenv('REDIS_HOST') === false ? 'localhost' : getenv('REDIS_HOST');
        $port = getenv('REDIS_PORT') === false ? '6379' : getenv('REDIS_PORT');

        try {
            RedisAdapter::createConnection("redis://{$host}:{$port}/0", ['retry_interval' => 1, 'timeout' => 1]);
        } catch (Exception) {
            self::markTestSkipped('Redis server is not available.');
        }
    }

    public function test_round_trips_rows_across_chunks_and_removes_the_bucket(): void
    {
        $host = getenv('REDIS_HOST') === false ? 'localhost' : getenv('REDIS_HOST');
        $port = getenv('REDIS_PORT') === false ? '6379' : getenv('REDIS_PORT');

        $cache = new Psr16Cache(new RedisAdapter(RedisAdapter::createConnection("redis://{$host}:{$port}/0", [
            'retry_interval' => 2,
            'timeout' => 5,
        ])));

        $storage = new PSRCacheBuckets($cache, prefix: 'flow:buckets:test:round_trip');
        $storage->append('bucket', rows(row(int_entry('id', 1)), row(int_entry('id', 2))));
        $storage->append('bucket', rows(row(int_entry('id', 3))));

        static::assertSame(
            [1, 2, 3],
            array_map(static fn(Row $r): mixed => $r->valueOf(
                'id',
            ), BucketsStorageContext::rows($storage->get('bucket'))),
        );

        $storage->remove('bucket');

        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));
    }
}
