<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Cache;
use Flow\ETL\DSL\Entry;
use Flow\ETL\DSL\From;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class CacheExtractorTest extends TestCase
{
    public function test_extracting_from_cache() : void
    {
        $cache = $this->createMock(Cache::class);

        $generator = function () : \Generator {
            yield new Rows(Row::create(Entry::integer('id', 1)));
            yield new Rows(Row::create(Entry::integer('id', 2)));
            yield new Rows(Row::create(Entry::integer('id', 3)));
        };

        $cache->expects($this->any())
            ->method('read')
            ->with('id')
            ->willReturn($generator());

        $cache->expects($this->never())
            ->method('clear')
            ->with('id');

        $extractor = From::cache('id', $cache);

        $this->assertEquals(
            [
                new Rows(Row::create(Entry::integer('id', 1))),
                new Rows(Row::create(Entry::integer('id', 2))),
                new Rows(Row::create(Entry::integer('id', 3))),
            ],
            \iterator_to_array($extractor->extract())
        );
    }

    public function test_extracting_from_cache_and_clear() : void
    {
        $cache = $this->createMock(Cache::class);

        $generator = function () : \Generator {
            yield new Rows(Row::create(Entry::integer('id', 1)));
            yield new Rows(Row::create(Entry::integer('id', 2)));
            yield new Rows(Row::create(Entry::integer('id', 3)));
        };

        $cache->expects($this->any())
            ->method('read')
            ->with('id')
            ->willReturn($generator());

        $cache->expects($this->once())
            ->method('clear')
            ->with('id');

        \iterator_to_array((From::cache('id', $cache, $clear = true))->extract());
    }
}
