<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Sort\ExternalSort;

use Flow\ETL\Sort\ExternalSort\BucketsCache\FilesystemBucketsCache;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class FilesystemBucketsCacheTest extends FlowIntegrationTestCase
{
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

        static::assertEquals($input, iterator_to_array($cache->get('bucket'), false));

        $this->fs()->rm($cacheDir);
    }
}
