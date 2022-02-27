<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache\LocalFilesystemCache;
use Flow\ETL\Config;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class LocalFilesystemCacheTest extends IntegrationTestCase
{
    public function test_writing_to_cache() : void
    {
        $cache = new LocalFilesystemCache($this->cacheDir, Config::default()->serializer());

        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 2))));

        $this->assertCount(
            2,
            \iterator_to_array($cache->read('test'))
        );
    }

    public function test_writing_to_cache_with_different_ids() : void
    {
        $cache = new LocalFilesystemCache($this->cacheDir, Config::default()->serializer());

        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->add('other-test', new Rows(Row::create(new IntegerEntry('id', 2))));

        $this->assertCount(
            1,
            \iterator_to_array($cache->read('test'))
        );
        $this->assertCount(
            1,
            \iterator_to_array($cache->read('other-test'))
        );
    }

    public function test_reading_empty_cache() : void
    {
        $cache = new LocalFilesystemCache($this->cacheDir, Config::default()->serializer());

        $this->assertCount(
            0,
            \iterator_to_array($cache->read('test'))
        );
    }

    public function test_cleaning_cache() : void
    {
        $cache = new LocalFilesystemCache($this->cacheDir, Config::default()->serializer());

        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 2))));

        $cache->clear('test');

        $this->assertCount(
            0,
            \iterator_to_array($cache->read('test'))
        );
    }

    public function test_cleaning_different_cache_id() : void
    {
        $cache = new LocalFilesystemCache($this->cacheDir, Config::default()->serializer());

        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->add('other-test', new Rows(Row::create(new IntegerEntry('id', 2))));

        $cache->clear('other-test');

        $this->assertCount(
            1,
            \iterator_to_array($cache->read('test'))
        );
    }
}
