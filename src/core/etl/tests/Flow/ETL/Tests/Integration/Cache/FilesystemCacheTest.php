<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache;
use Flow\ETL\Cache\Implementation\FilesystemCache;
use Flow\ETL\Exception\KeyNotInCacheException;

use function file_put_contents;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\Filesystem\DSL\path;
use function glob;

final class FilesystemCacheTest extends CacheTestCase
{
    public function test_torn_entry_is_treated_as_a_cache_miss(): void
    {
        $cache = $this->cache();
        $cache->set('torn', rows(row(int_entry('id', 1))));

        // simulate a crash mid-write / bit rot: overwrite the closed .floe file with garbage
        $files = glob(__DIR__ . '/var/filesystem-cache/*/*/*/*/torn.floe') ?: [];
        static::assertNotEmpty($files);
        file_put_contents($files[0], 'not a valid floe file');

        $this->expectException(KeyNotInCacheException::class);

        $cache->get('torn');
    }

    protected function cache(): Cache
    {
        return new FilesystemCache($this->fs(), path(__DIR__ . '/var/filesystem-cache'));
    }
}
