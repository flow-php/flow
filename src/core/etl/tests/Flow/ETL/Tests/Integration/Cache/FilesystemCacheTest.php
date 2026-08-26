<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\FilesystemCache;
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Tests\Double\SpySerializer;

use function file_get_contents;
use function file_put_contents;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\Filesystem\DSL\path;
use function glob;
use function unlink;

final class FilesystemCacheTest extends CacheTestCase
{
    public function test_torn_entry_is_treated_as_a_cache_miss(): void
    {
        $cache = $this->cache();
        $cache->set('torn', rows(row(int_entry('id', 1))));

        // simulate a crash mid-write / bit rot: overwrite the closed cache file with garbage
        $files = glob(__DIR__ . '/var/filesystem-cache/*/*/*/*/torn') ?: [];
        static::assertNotEmpty($files);
        file_put_contents($files[0], 'not a valid floe file');

        $this->expectException(KeyNotInCacheException::class);

        $cache->get('torn');
    }

    public function test_corrupted_frame_with_intact_footer_is_treated_as_a_cache_miss(): void
    {
        $cache = $this->cache();
        $cache->set('corrupt', rows(row(int_entry('id', 1))));

        $files = glob(__DIR__ . '/var/filesystem-cache/*/*/*/*/corrupt') ?: [];
        static::assertNotEmpty($files);

        // corrupt the first frame type (SCHEMA -> unknown 0x7F); the footer at the end stays
        // intact, so the recovered row count no longer matches the footer's totalRows
        $bytes = (string) file_get_contents($files[0]);
        $bytes[6] = "\x7F";
        file_put_contents($files[0], $bytes);

        $this->expectException(KeyNotInCacheException::class);

        $cache->get('corrupt');
    }

    public function test_custom_serializer_is_used_for_set_and_get(): void
    {
        $spy = new SpySerializer();
        $cache = new FilesystemCache($this->fs(), path(__DIR__ . '/var/filesystem-cache-spy'), $spy);
        $cache->clear();

        $cache->set('spy', $rows = rows(row(int_entry('id', 1)), row(int_entry('id', 2))));

        static::assertEquals($rows, $cache->get('spy'));
        static::assertCount(1, $spy->serialized);
        static::assertCount(1, $spy->unserialized);

        $cache->clear();
    }

    public function test_schema_of_an_entry_without_a_stored_schema_is_a_cache_miss(): void
    {
        $cache = $this->cache();
        $cache->set('orphan', rows(row(int_entry('id', 1))));

        $files = glob(__DIR__ . '/var/filesystem-cache/*/*/*/*/orphan.schema') ?: [];
        static::assertNotEmpty($files);
        unlink($files[0]);

        $this->expectException(KeyNotInCacheException::class);

        $cache->schema('orphan');
    }

    public function test_torn_schema_is_treated_as_a_cache_miss(): void
    {
        $cache = $this->cache();
        $cache->set('torn-schema', rows(row(int_entry('id', 1))));

        $files = glob(__DIR__ . '/var/filesystem-cache/*/*/*/*/torn-schema.schema') ?: [];
        static::assertNotEmpty($files);
        file_put_contents($files[0], 'not a valid schema payload');

        $this->expectException(KeyNotInCacheException::class);

        $cache->schema('torn-schema');
    }

    protected function cache(): Cache
    {
        return new FilesystemCache($this->fs(), path(__DIR__ . '/var/filesystem-cache'));
    }
}
