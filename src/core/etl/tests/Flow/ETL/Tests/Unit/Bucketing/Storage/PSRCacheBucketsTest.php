<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing\Storage;

use Flow\ETL\Bucketing\Storage\PSRCacheBuckets;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\Double\ArrayCache;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Floe\FloeSerializer;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class PSRCacheBucketsTest extends FlowTestCase
{
    public function test_append_then_get_returns_rows_in_order_across_chunks(): void
    {
        $storage = new PSRCacheBuckets(new ArrayCache(), new FloeSerializer(new AdaptiveBackend()));
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 3]], schema(int_schema('id'))));

        static::assertSame([1, 2, 3], array_column(BucketsStorageContext::rows($storage->get('bucket')), 'id'));
    }

    public function test_custom_prefix_is_used_in_keys(): void
    {
        $cache = new ArrayCache();
        (new PSRCacheBuckets(
            $cache,
            new FloeSerializer(new AdaptiveBackend()),
            prefix: 'custom:pfx',
        ))->append('bucket', array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id'))));

        static::assertTrue($cache->has('custom:pfx:bucket:chunks'));
        static::assertTrue($cache->has('custom:pfx:bucket:chunk:0'));
    }

    public function test_get_skips_non_string_chunk_payloads(): void
    {
        $cache = new ArrayCache();
        $cache->set('flow:buckets:bucket:chunks', 1);
        $cache->set('flow:buckets:bucket:chunk:0', 123);

        static::assertSame(
            [],
            BucketsStorageContext::rows((new PSRCacheBuckets($cache, new FloeSerializer(new AdaptiveBackend())))->get(
                'bucket',
            )),
        );
    }

    public function test_get_treats_non_integer_chunk_counter_as_empty(): void
    {
        $cache = new ArrayCache();
        $cache->set('flow:buckets:bucket:chunks', 'garbage');

        static::assertSame(
            [],
            BucketsStorageContext::rows((new PSRCacheBuckets($cache, new FloeSerializer(new AdaptiveBackend())))->get(
                'bucket',
            )),
        );
    }

    public function test_get_unknown_bucket_yields_nothing(): void
    {
        static::assertSame(
            [],
            BucketsStorageContext::rows((new PSRCacheBuckets(
                new ArrayCache(),
                new FloeSerializer(new AdaptiveBackend()),
            ))->get('unknown')),
        );
    }

    public function test_remove_deletes_all_chunk_keys_and_the_counter(): void
    {
        $cache = new ArrayCache();
        $storage = new PSRCacheBuckets($cache, new FloeSerializer(new AdaptiveBackend()));
        $storage->append('bucket', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $storage->append('bucket', array_to_rows([['id' => 2]], schema(int_schema('id'))));

        $storage->remove('bucket');

        static::assertFalse($cache->has('flow:buckets:bucket:chunks'));
        static::assertFalse($cache->has('flow:buckets:bucket:chunk:0'));
        static::assertFalse($cache->has('flow:buckets:bucket:chunk:1'));
        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));
    }

    public function test_round_trips_schema_changing_rows(): void
    {
        $storage = new PSRCacheBuckets(new ArrayCache(), new FloeSerializer(new AdaptiveBackend()));
        $input = array_to_rows(
            [['id' => 1], ['id' => 2, 'name' => 'John'], ['name' => 'Jane']],
            schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
        );
        $storage->append('bucket', $input);

        static::assertSame($input->toArray(), BucketsStorageContext::rows($storage->get('bucket')));
    }

    public function test_set_replaces_previous_content(): void
    {
        $storage = new PSRCacheBuckets(new ArrayCache(), new FloeSerializer(new AdaptiveBackend()));
        $storage->append('bucket', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $storage->set('bucket', array_to_rows([['id' => 42]], schema(int_schema('id'))));

        static::assertSame([42], array_column(BucketsStorageContext::rows($storage->get('bucket')), 'id'));
    }
}
