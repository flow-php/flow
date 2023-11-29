<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Cache;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Cache\InMemoryCache;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class IneMemoryCacheTest extends TestCase
{
    public function test_cache() : void
    {
        $cache = new InMemoryCache();

        $this->assertFalse($cache->has('id-1'));
        $this->assertFalse($cache->has('id-2'));

        $cache->add('id-1', $rows1 = new Rows(Row::create(int_entry('id', 1))));
        $cache->add('id-2', $rows2 = new Rows(Row::create(int_entry('id', 1))));

        $this->assertEquals([$rows1], \iterator_to_array($cache->read('id-1')));
        $this->assertEquals([$rows2], \iterator_to_array($cache->read('id-2')));

        $cache->clear('id-1');
        $cache->clear('id-2');

        $this->assertEquals([], \iterator_to_array($cache->read('id-1')));
        $this->assertEquals([], \iterator_to_array($cache->read('id-2')));
        $this->assertTrue($cache->has('id-1'));
        $this->assertTrue($cache->has('id-2'));
    }
}
