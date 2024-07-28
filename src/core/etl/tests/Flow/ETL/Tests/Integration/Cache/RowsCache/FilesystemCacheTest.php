<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache\RowsCache;

use function Flow\Filesystem\DSL\protocol;
use Flow\ETL\Cache\RowsCache\FilesystemCache;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Flow\ETL\{Row, Rows};
use Flow\Serializer\{Base64Serializer, CompressingSerializer, NativePHPSerializer};

final class FilesystemCacheTest extends IntegrationTestCase
{
    public function test_cleaning_cache() : void
    {
        $cache = new FilesystemCache(
            $this->fstab()->for(protocol('file')),
            new Base64Serializer(new CompressingSerializer(new NativePHPSerializer())),
            $this->cacheDir,
        );

        self::assertFalse($cache->has('test'));

        $cache->append('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->append('test', new Rows(Row::create(new IntegerEntry('id', 2))));

        self::assertTrue($cache->has('test'));

        $cache->remove('test');

        self::assertCount(
            0,
            \iterator_to_array($cache->get('test'))
        );
        self::assertFalse($cache->has('test'));
    }

    public function test_cleaning_different_cache_id() : void
    {
        $cache = new FilesystemCache(
            $this->fstab()->for(protocol('file')),
            new Base64Serializer(new CompressingSerializer(new NativePHPSerializer())),
            $this->cacheDir,
        );

        $cache->append('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->append('other-test', new Rows(Row::create(new IntegerEntry('id', 2))));

        $cache->remove('other-test');

        self::assertCount(
            1,
            \iterator_to_array($cache->get('test'))
        );
    }

    public function test_reading_empty_cache() : void
    {
        $cache = new FilesystemCache(
            $this->fstab()->for(protocol('file')),
            new Base64Serializer(new CompressingSerializer(new NativePHPSerializer())),
            $this->cacheDir,
        );

        self::assertCount(
            0,
            \iterator_to_array($cache->get('test'))
        );
    }

    public function test_writing_to_cache() : void
    {
        $cache = new FilesystemCache(
            $this->fstab()->for(protocol('file')),
            new Base64Serializer(new CompressingSerializer(new NativePHPSerializer())),
            $this->cacheDir,
        );

        $cache->append('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->append('test', new Rows(Row::create(new IntegerEntry('id', 2))));

        self::assertCount(
            2,
            \iterator_to_array($cache->get('test'))
        );
    }

    public function test_writing_to_cache_with_different_ids() : void
    {
        $cache = new FilesystemCache(
            $this->fstab()->for(protocol('file')),
            new Base64Serializer(new CompressingSerializer(new NativePHPSerializer())),
            $this->cacheDir,
        );

        $cache->append('test', new Rows(Row::create(new IntegerEntry('id', 1))));
        $cache->append('other-test', new Rows(Row::create(new IntegerEntry('id', 2))));

        self::assertCount(
            1,
            \iterator_to_array($cache->get('test'))
        );
        self::assertCount(
            1,
            \iterator_to_array($cache->get('other-test'))
        );
    }
}
