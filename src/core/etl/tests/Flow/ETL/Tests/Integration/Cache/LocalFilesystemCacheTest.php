<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use Flow\ETL\Cache\RowsCache\LocalFilesystemCache;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Flow\ETL\{Row, Rows};
use Flow\Serializer\{Base64Serializer, CompressingSerializer, NativePHPSerializer};

final class LocalFilesystemCacheTest extends IntegrationTestCase
{
    public function test_cleaning_cache() : void
    {
        $cache = new LocalFilesystemCache($this->cacheDir, new Base64Serializer(new CompressingSerializer(new NativePHPSerializer())));

        self::assertFalse($cache->has('test'));

        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 2))));

        self::assertTrue($cache->has('test'));

        $cache->clear('test');

        self::assertCount(
            0,
            \iterator_to_array($cache->read('test'))
        );
        self::assertFalse($cache->has('test'));
    }

    public function test_cleaning_different_cache_id() : void
    {
        $cache = new LocalFilesystemCache($this->cacheDir, new Base64Serializer(new CompressingSerializer(new NativePHPSerializer())));

        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->add('other-test', new Rows(Row::create(new IntegerEntry('id', 2))));

        $cache->clear('other-test');

        self::assertCount(
            1,
            \iterator_to_array($cache->read('test'))
        );
    }

    public function test_reading_empty_cache() : void
    {
        $cache = new LocalFilesystemCache($this->cacheDir, new Base64Serializer(new CompressingSerializer(new NativePHPSerializer())));

        self::assertCount(
            0,
            \iterator_to_array($cache->read('test'))
        );
    }

    public function test_writing_to_cache() : void
    {
        $cache = new LocalFilesystemCache($this->cacheDir, new Base64Serializer(new CompressingSerializer(new NativePHPSerializer())));

        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 2))));

        self::assertCount(
            2,
            \iterator_to_array($cache->read('test'))
        );
    }

    public function test_writing_to_cache_with_different_ids() : void
    {
        $cache = new LocalFilesystemCache($this->cacheDir, new Base64Serializer(new CompressingSerializer(new NativePHPSerializer())));

        $cache->add('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->add('other-test', new Rows(Row::create(new IntegerEntry('id', 2))));

        self::assertCount(
            1,
            \iterator_to_array($cache->read('test'))
        );
        self::assertCount(
            1,
            \iterator_to_array($cache->read('other-test'))
        );
    }
}
