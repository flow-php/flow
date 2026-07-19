<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Sort\ExternalSort;

use Flow\ETL\Row;
use Flow\ETL\Sort\ExternalSort\BucketsCache\FilesystemBucketsCache;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function array_map;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class FilesystemBucketsCacheTest extends FlowIntegrationTestCase
{
    public function test_appends_accumulate_into_one_bucket_in_order(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append');
        $this->fs()->rm($cacheDir);

        $cache = new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir, batchSize: 2);
        $cache->append('bucket', [row(int_entry('id', 1)), row(int_entry('id', 2))]);
        $cache->append('bucket', [row(int_entry('id', 3))]);
        $cache->append('bucket', [row(int_entry('id', 4)), row(int_entry('id', 5))]);

        $ids = [];

        foreach ($cache->get('bucket') as $row) {
            $ids[] = $row->valueOf('id');
        }

        static::assertSame([1, 2, 3, 4, 5], $ids);

        $this->fs()->rm($cacheDir);
    }

    public function test_append_after_get_does_not_truncate_the_bucket(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_reopen');
        $this->fs()->rm($cacheDir);

        $cache = new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir);
        $cache->append('bucket', [row(int_entry('id', 1))]);

        static::assertCount(1, iterator_to_array($cache->get('bucket'), false));

        $cache->append('bucket', [row(int_entry('id', 2))]);

        static::assertCount(2, iterator_to_array($cache->get('bucket'), false));

        $this->fs()->rm($cacheDir);
    }

    public function test_remove_closes_an_open_append_session(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_remove');
        $this->fs()->rm($cacheDir);

        $cache = new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir);
        $cache->append('bucket', [row(int_entry('id', 1))]);
        $cache->remove('bucket');

        static::assertSame([], iterator_to_array($cache->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_set_replaces_a_bucket_written_with_append(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_append_set');
        $this->fs()->rm($cacheDir);

        $cache = new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir);
        $cache->append('bucket', [row(int_entry('id', 1)), row(int_entry('id', 2))]);
        $cache->set('bucket', [row(int_entry('id', 3))]);

        $ids = [];

        foreach ($cache->get('bucket') as $row) {
            $ids[] = $row->valueOf('id');
        }

        static::assertSame([3], $ids);

        $this->fs()->rm($cacheDir);
    }

    public function test_custom_batch_size_round_trips_all_rows(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_batch_size');
        $this->fs()->rm($cacheDir);

        $cache = new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir, batchSize: 2);
        $cache->set('bucket', [row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3))]);

        static::assertCount(3, iterator_to_array($cache->get('bucket'), false));
    }

    public function test_get_missing_bucket_yields_nothing(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_missing');
        $this->fs()->rm($cacheDir);

        $cache = new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir);

        static::assertSame([], iterator_to_array($cache->get('nope')));
    }

    public function test_remove_deletes_the_bucket(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_remove');
        $this->fs()->rm($cacheDir);

        $cache = new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir);
        $cache->set('bucket', [row(int_entry('id', 1))]);
        $cache->remove('bucket');

        static::assertSame([], iterator_to_array($cache->get('bucket')));

        $this->fs()->rm($cacheDir);
    }

    public function test_round_trips_rows_across_the_write_batch_boundary(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_batch');
        $this->fs()->rm($cacheDir);

        $cache = new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir);

        $input = [];

        for ($i = 0; $i < 1500; $i++) {
            $input[] = row(int_entry('id', $i));
        }

        $cache->set('bucket', $input);

        static::assertEquals($input, iterator_to_array($cache->get('bucket'), false));

        $this->fs()->rm($cacheDir);
    }

    public function test_append_round_trips_a_nullable_column_present_then_null_across_batches(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_present_then_null');
        $this->fs()->rm($cacheDir);

        $cache = new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir, batchSize: 1);
        $cache->append('bucket', [row(int_entry('id', 1), str_entry('opt', 'present'))]);
        $cache->append('bucket', [row(int_entry('id', 2), str_entry('opt', null))]);

        static::assertSame(
            [['id' => 1, 'opt' => 'present'], ['id' => 2, 'opt' => null]],
            array_map(static fn(Row $r): array => $r->toArray(), iterator_to_array($cache->get('bucket'), false)),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_round_trips_schema_changing_rows(): void
    {
        $cacheDir = path(__DIR__ . '/var/buckets_heterogeneous');
        $this->fs()->rm($cacheDir);

        $cache = new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir);

        $input = [
            row(int_entry('id', 1)),
            row(int_entry('id', 2), str_entry('name', 'John')),
            row(str_entry('name', 'Jane')),
        ];

        $cache->set('bucket', $input);

        // one write session = one schema: rows keep their own columns (unpadded) but entry
        // types widen to the batch union, so compare values rather than exact definitions
        $result = iterator_to_array($cache->get('bucket'), false);
        static::assertSame(
            array_map(static fn(Row $r): array => $r->toArray(), $input),
            array_map(static fn(Row $r): array => $r->toArray(), $result),
        );

        $this->fs()->rm($cacheDir);
    }
}
