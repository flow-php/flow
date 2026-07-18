<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort\ExternalSort\BucketsCache;

use Flow\ETL\Sort\ExternalSort\BucketsCache\InMemoryBucketsCache;
use Flow\ETL\Sort\ExternalSort\ResidentBucketsCache;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function iterator_to_array;

final class InMemoryBucketsCacheTest extends FlowTestCase
{
    public function test_append_accumulates_rows(): void
    {
        $cache = new InMemoryBucketsCache();

        $cache->append('bucket', rows(row(int_entry('id', 1))));
        $cache->append('bucket', rows(row(int_entry('id', 2))));

        /** @var list<\Flow\ETL\Row> $bucketRows */
        $bucketRows = iterator_to_array($cache->get('bucket'), false);

        static::assertCount(2, $bucketRows);
        static::assertSame(1, $bucketRows[0]->valueOf('id'));
        static::assertSame(2, $bucketRows[1]->valueOf('id'));
    }

    public function test_get_unknown_bucket_yields_nothing(): void
    {
        static::assertSame([], iterator_to_array((new InMemoryBucketsCache())->get('unknown'), false));
    }

    public function test_is_resident(): void
    {
        static::assertInstanceOf(ResidentBucketsCache::class, new InMemoryBucketsCache());
    }

    public function test_remove_drops_the_bucket(): void
    {
        $cache = new InMemoryBucketsCache();

        $cache->append('bucket', rows(row(int_entry('id', 1))));
        $cache->remove('bucket');

        static::assertSame([], iterator_to_array($cache->get('bucket'), false));
    }

    public function test_set_replaces_previous_rows(): void
    {
        $cache = new InMemoryBucketsCache();

        $cache->append('bucket', rows(row(int_entry('id', 1))));
        $cache->set('bucket', rows(row(int_entry('id', 42))));

        /** @var list<\Flow\ETL\Row> $bucketRows */
        $bucketRows = iterator_to_array($cache->get('bucket'), false);

        static::assertCount(1, $bucketRows);
        static::assertSame(42, $bucketRows[0]->valueOf('id'));
    }
}
