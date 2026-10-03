<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Bucketing\Storage;

use Exception;
use Flow\ETL\Bucketing\Storage\PSRCacheBuckets;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\FloeSerializer;
use Override;
use Symfony\Component\Cache\Adapter\RedisAdapter;
use Symfony\Component\Cache\Psr16Cache;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
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

        $storage = new PSRCacheBuckets(
            $cache,
            new FloeSerializer(new AdaptiveBackend()),
            prefix: 'flow:buckets:test:round_trip',
        );
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 3]], schema(int_schema('id'))));

        static::assertSame([1, 2, 3], array_column(BucketsStorageContext::rows($storage->get('bucket')), 'id'));

        $storage->remove('bucket');

        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));
    }
}
