<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Cache;

use Flow\ETL\Cache\InMemoryCache;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class IneMemoryCacheTest extends TestCase
{
    public function test_cache() : void
    {
        $cache = new InMemoryCache();

        $cache->add('id-1', $rows1 = new Rows(Row::create(Entry::integer('id', 1))));
        $cache->add('id-2', $rows2 = new Rows(Row::create(Entry::integer('id', 1))));

        $this->assertEquals([$rows1], \iterator_to_array($cache->read('id-1')));
        $this->assertEquals([$rows2], \iterator_to_array($cache->read('id-2')));

        $cache->clear('id-1');
        $cache->clear('id-2');

        $this->assertEquals([], \iterator_to_array($cache->read('id-1')));
        $this->assertEquals([], \iterator_to_array($cache->read('id-2')));
    }
}
