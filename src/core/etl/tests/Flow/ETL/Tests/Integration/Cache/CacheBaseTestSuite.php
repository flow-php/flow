<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Cache;

use function Flow\ETL\DSL\{row, rows, str_entry};
use Flow\ETL\Cache;
use Flow\ETL\Cache\{CacheIndex};
use Flow\ETL\Exception\KeyNotInCacheException;
use Flow\ETL\Tests\FlowIntegrationTestCase;

abstract class CacheBaseTestSuite extends FlowIntegrationTestCase
{
    protected function setUp() : void
    {
        parent::setUp();

        $this->cache()->clear();
    }

    protected function tearDown() : void
    {
        parent::tearDown();

        $this->cache()->clear();
    }

    public function test_caching_index() : void
    {
        $cache = $this->cache();

        static::assertFalse($cache->has('index'));

        $cache->set('index', $index = new CacheIndex('index'));

        static::assertTrue($cache->has('index'));

        static::assertEquals($index, $cache->get('index'));
    }

    public function test_caching_row() : void
    {
        $cache = $this->cache();

        static::assertFalse($cache->has('row'));

        $cache->set('row', $row = row(str_entry('name', 'John')));

        static::assertTrue($cache->has('row'));

        static::assertEquals($row, $cache->get('row'));
    }

    public function test_caching_rows() : void
    {
        $cache = $this->cache();

        static::assertFalse($cache->has('rows'));

        $cache->set('rows', $rows = rows(row(str_entry('name', 'John')), row(str_entry('name', 'Jane'))));

        static::assertTrue($cache->has('rows'));

        static::assertEquals($rows, $cache->get('rows'));
    }

    public function test_checking_on_non_existing_cache_key() : void
    {
        $cache = $this->cache();

        static::assertFalse($cache->has('non-existing'));
    }

    public function test_clearing_cache() : void
    {
        $cache = $this->cache();

        $cache->set('index', new CacheIndex('index'));
        $cache->set('row', row(str_entry('name', 'John')));
        $cache->set('rows', rows(row(str_entry('name', 'John')), row(str_entry('name', 'Jane'))));

        $cache->clear();

        static::assertFalse($cache->has('index'));
        static::assertFalse($cache->has('row'));
        static::assertFalse($cache->has('rows'));
    }

    public function test_getting_non_existing_cache_key() : void
    {
        $cache = $this->cache();

        $this->expectException(KeyNotInCacheException::class);

        $cache->get('non-existing');
    }

    public function test_removing_from_cache() : void
    {
        $cache = $this->cache();

        $cache->set('index', new CacheIndex('index'));
        $cache->set('row', row(str_entry('name', 'John')));
        $cache->set('rows', rows(row(str_entry('name', 'John')), row(str_entry('name', 'Jane'))));

        $cache->delete('row');

        static::assertTrue($cache->has('index'));
        static::assertFalse($cache->has('row'));
        static::assertTrue($cache->has('rows'));
    }

    public function test_removing_non_existing_cache_key() : void
    {
        $cache = $this->cache();

        $cache->delete('non-existing');

        static::assertFalse($cache->has('non-existing'));
    }

    abstract protected function cache() : Cache;
}
