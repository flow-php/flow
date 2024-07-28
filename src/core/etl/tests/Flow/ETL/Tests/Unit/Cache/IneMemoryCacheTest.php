<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Cache;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Cache\RowsCache\InMemoryCache;
use Flow\ETL\{Row, Rows};
use PHPUnit\Framework\TestCase;

final class IneMemoryCacheTest extends TestCase
{
    public function test_cache() : void
    {
        $cache = new InMemoryCache();

        self::assertFalse($cache->has('id-1'));
        self::assertFalse($cache->has('id-2'));

        $cache->append('id-1', $rows1 = new Rows(Row::create(int_entry('id', 1))));
        $cache->append('id-2', $rows2 = new Rows(Row::create(int_entry('id', 1))));

        self::assertEquals([$rows1], \iterator_to_array($cache->get('id-1')));
        self::assertEquals([$rows2], \iterator_to_array($cache->get('id-2')));

        $cache->remove('id-1');
        $cache->remove('id-2');

        self::assertEquals([], \iterator_to_array($cache->get('id-1')));
        self::assertEquals([], \iterator_to_array($cache->get('id-2')));
        self::assertTrue($cache->has('id-1'));
        self::assertTrue($cache->has('id-2'));
    }
}
