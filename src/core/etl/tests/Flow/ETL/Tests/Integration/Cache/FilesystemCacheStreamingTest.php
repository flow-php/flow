<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\FilesystemCache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Row;
use Flow\ETL\Rows;

use function array_map;
use function file_get_contents;
use function file_put_contents;
use function Flow\ETL\DSL\filesystem_cache;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\Filesystem\DSL\path;
use function glob;
use function iterator_to_array;
use function range;

final class FilesystemCacheStreamingTest extends CacheTestCase
{
    public function test_both_modes_write_identical_files(): void
    {
        $rows = rows(...array_map(static fn(int $id): Row => row(int_entry('id', $id)), range(1, 10)));

        $bulkCache = new FilesystemCache($this->fs(), path(__DIR__ . '/var/filesystem-cache-bulk'));

        $this->cache()->set('parity', $rows);
        $bulkCache->set('parity', $rows);

        $streamingFiles = glob(__DIR__ . '/var/filesystem-cache-streaming/*/*/*/*/parity.floe') ?: [];
        $bulkFiles = glob(__DIR__ . '/var/filesystem-cache-bulk/*/*/*/*/parity.floe') ?: [];

        static::assertNotEmpty($streamingFiles);
        static::assertNotEmpty($bulkFiles);
        static::assertSame(file_get_contents($bulkFiles[0]), file_get_contents($streamingFiles[0]));

        $bulkCache->clear();
    }

    public function test_filesystem_cache_dsl_delegates_mode_and_batch_size(): void
    {
        $cache = filesystem_cache(path(__DIR__ . '/var/filesystem-cache-dsl'), serializer_batch_size: 2);

        $cache->set('dsl', rows(...array_map(static fn(int $id): Row => row(int_entry('id', $id)), range(1, 5))));

        static::assertCount(3, iterator_to_array($cache->read('dsl'), preserve_keys: false));

        $cache->clear();
    }

    public function test_reading_yields_batches_of_batch_size(): void
    {
        $cache = $this->cache();

        $cache->set('batched', rows(...array_map(static fn(int $id): Row => row(int_entry('id', $id)), range(1, 10))));

        $batches = iterator_to_array($cache->read('batched'), preserve_keys: false);

        static::assertCount(4, $batches);
        static::assertSame([3, 3, 3, 1], array_map(static fn(Rows $batch): int => $batch->count(), $batches));
    }

    public function test_torn_entry_is_treated_as_a_cache_miss(): void
    {
        $cache = $this->cache();
        $cache->set('torn', rows(row(int_entry('id', 1))));

        $files = glob(__DIR__ . '/var/filesystem-cache-streaming/*/*/*/*/torn.floe') ?: [];
        static::assertNotEmpty($files);
        file_put_contents($files[0], 'not a valid floe file');

        $this->expectException(KeyNotInCacheException::class);

        $cache->get('torn');
    }

    public function test_corrupted_frame_with_intact_footer_is_treated_as_a_cache_miss(): void
    {
        $cache = $this->cache();
        $cache->set('corrupt', rows(row(int_entry('id', 1))));

        $files = glob(__DIR__ . '/var/filesystem-cache-streaming/*/*/*/*/corrupt.floe') ?: [];
        static::assertNotEmpty($files);

        // corrupt the first frame type (SCHEMA -> unknown 0x7F); the footer at the end stays
        // intact, so the recovered row count no longer matches the footer's totalRows
        $bytes = (string) file_get_contents($files[0]);
        $bytes[6] = "\x7F";
        file_put_contents($files[0], $bytes);

        $this->expectException(KeyNotInCacheException::class);

        $cache->get('corrupt');
    }

    public function test_torn_entry_read_is_treated_as_a_cache_miss(): void
    {
        $cache = $this->cache();
        $cache->set('torn-read', rows(row(int_entry('id', 1))));

        $files = glob(__DIR__ . '/var/filesystem-cache-streaming/*/*/*/*/torn-read.floe') ?: [];
        static::assertNotEmpty($files);
        file_put_contents($files[0], 'not a valid floe file');

        $this->expectException(KeyNotInCacheException::class);

        iterator_to_array($cache->read('torn-read'));
    }

    protected function cache(): Cache
    {
        return new FilesystemCache($this->fs(), path(__DIR__ . '/var/filesystem-cache-streaming'), 3);
    }
}
