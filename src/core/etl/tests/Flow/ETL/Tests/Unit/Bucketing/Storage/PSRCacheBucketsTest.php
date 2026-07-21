<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Bucketing\Storage;

use Flow\ETL\Bucketing\Storage\PSRCacheBuckets;
use Flow\ETL\Row;
use Flow\ETL\Tests\Context\BucketsStorageContext;
use Flow\ETL\Tests\Double\ArrayCache;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function iterator_to_array;

final class PSRCacheBucketsTest extends FlowTestCase
{
    public function test_append_then_get_returns_rows_in_order_across_chunks(): void
    {
        $storage = new PSRCacheBuckets(new ArrayCache());
        $storage->append('bucket', rows(row(int_entry('id', 1)), row(int_entry('id', 2))));
        $storage->append('bucket', rows(row(int_entry('id', 3))));

        static::assertSame(
            [1, 2, 3],
            array_map(static fn(Row $r): mixed => $r->valueOf(
                'id',
            ), BucketsStorageContext::rows($storage->get('bucket'))),
        );
    }

    public function test_custom_prefix_is_used_in_keys(): void
    {
        $cache = new ArrayCache();
        (new PSRCacheBuckets($cache, prefix: 'custom:pfx'))->append('bucket', rows(row(int_entry('id', 1))));

        static::assertTrue($cache->has('custom:pfx:bucket:chunks'));
        static::assertTrue($cache->has('custom:pfx:bucket:chunk:0'));
    }

    public function test_get_skips_non_string_chunk_payloads(): void
    {
        $cache = new ArrayCache();
        $cache->set('flow:buckets:bucket:chunks', 1);
        $cache->set('flow:buckets:bucket:chunk:0', 123);

        static::assertSame([], BucketsStorageContext::rows((new PSRCacheBuckets($cache))->get('bucket')));
    }

    public function test_get_treats_non_integer_chunk_counter_as_empty(): void
    {
        $cache = new ArrayCache();
        $cache->set('flow:buckets:bucket:chunks', 'garbage');

        static::assertSame([], BucketsStorageContext::rows((new PSRCacheBuckets($cache))->get('bucket')));
    }

    public function test_get_unknown_bucket_yields_nothing(): void
    {
        static::assertSame([], BucketsStorageContext::rows((new PSRCacheBuckets(new ArrayCache()))->get('unknown')));
    }

    public function test_remove_deletes_all_chunk_keys_and_the_counter(): void
    {
        $cache = new ArrayCache();
        $storage = new PSRCacheBuckets($cache);
        $storage->append('bucket', rows(row(int_entry('id', 1))));
        $storage->append('bucket', rows(row(int_entry('id', 2))));

        $storage->remove('bucket');

        static::assertFalse($cache->has('flow:buckets:bucket:chunks'));
        static::assertFalse($cache->has('flow:buckets:bucket:chunk:0'));
        static::assertFalse($cache->has('flow:buckets:bucket:chunk:1'));
        static::assertSame([], BucketsStorageContext::rows($storage->get('bucket')));
    }

    public function test_round_trips_schema_changing_rows(): void
    {
        $storage = new PSRCacheBuckets(new ArrayCache());
        $input = rows(
            row(int_entry('id', 1)),
            row(int_entry('id', 2), str_entry('name', 'John')),
            row(str_entry('name', 'Jane')),
        );
        $storage->append('bucket', $input);

        static::assertSame(
            array_map(static fn(Row $r): array => $r->toArray(), iterator_to_array($input, false)),
            array_map(static fn(Row $r): array => $r->toArray(), BucketsStorageContext::rows($storage->get('bucket'))),
        );
    }

    public function test_set_replaces_previous_content(): void
    {
        $storage = new PSRCacheBuckets(new ArrayCache());
        $storage->append('bucket', rows(row(int_entry('id', 1)), row(int_entry('id', 2))));
        $storage->set('bucket', rows(row(int_entry('id', 42))));

        static::assertSame(
            [42],
            array_map(static fn(Row $r): mixed => $r->valueOf(
                'id',
            ), BucketsStorageContext::rows($storage->get('bucket'))),
        );
    }
}
