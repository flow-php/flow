<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Bucketing\Storage;

use Flow\ETL\Bucketing\Storage\PSRCacheBuckets;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Floe\FloeSerializer;
use Symfony\Component\Cache\Adapter\FilesystemAdapter;
use Symfony\Component\Cache\Psr16Cache;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class PSRCacheBucketsTest extends FlowIntegrationTestCase
{
    public function test_round_trips_rows_across_chunks_and_removes_the_bucket(): void
    {
        $storage = new PSRCacheBuckets(
            new Psr16Cache(new FilesystemAdapter(directory: __DIR__ . '/var/psr-cache-buckets')),
            new FloeSerializer(new AdaptiveBackend()),
        );
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 3]], schema(int_schema('id'))));

        static::assertSame([1, 2, 3], array_column(BucketsStorageContext::rows($storage->get('bucket')), 'id'));

        $storage->remove('bucket');

        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));
    }
}
