<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\{from_cache, from_rows, int_entry};
use Flow\ETL\{Cache, Config, FlowContext, Row, Rows};
use PHPUnit\Framework\TestCase;

final class CacheExtractorTest extends TestCase
{
    public function test_extracting_from_cache() : void
    {
        $cache = $this->createMock(Cache::class);

        $generator = function () : \Generator {
            yield new Rows(Row::create(int_entry('id', 1)));
            yield new Rows(Row::create(int_entry('id', 2)));
            yield new Rows(Row::create(int_entry('id', 3)));
        };

        $cache->expects(self::any())
            ->method('read')
            ->with('id')
            ->willReturn($generator());

        $cache->expects(self::any())
            ->method('has')
            ->with('id')
            ->willReturn(true);

        $cache->expects(self::never())
            ->method('clear')
            ->with('id');

        $extractor = from_cache('id');

        self::assertEquals(
            [
                new Rows(Row::create(int_entry('id', 1))),
                new Rows(Row::create(int_entry('id', 2))),
                new Rows(Row::create(int_entry('id', 3))),
            ],
            \iterator_to_array($extractor->extract(new FlowContext(Config::builder()->cache($cache)->build())))
        );
    }

    public function test_extracting_from_cache_and_clear() : void
    {
        $cache = $this->createMock(Cache::class);

        $generator = function () : \Generator {
            yield new Rows(Row::create(int_entry('id', 1)));
            yield new Rows(Row::create(int_entry('id', 2)));
            yield new Rows(Row::create(int_entry('id', 3)));
        };

        $cache->expects(self::any())
            ->method('read')
            ->with('id')
            ->willReturn($generator());

        $cache->expects(self::any())
            ->method('has')
            ->with('id')
            ->willReturn(true);

        $cache->expects(self::once())
            ->method('clear')
            ->with('id');

        \iterator_to_array((from_cache('id', clear: true))->extract(new FlowContext(Config::builder()->cache($cache)->build())));
    }

    public function test_extracting_from_fallback_extractor_when_cache_is_empty() : void
    {
        $config = Config::builder()
            ->cache($cache = $this->createMock(Cache::class))
            ->build();

        $cache->expects(self::exactly(2))
            ->method('has')
            ->with('id')
            ->willReturnOnConsecutiveCalls(false, true);

        $cache->expects(self::once())
            ->method('read')
            ->with('id')
            ->willReturn(
                from_rows($rowsToCache = new Rows(
                    Row::create(int_entry('id', 1)),
                    Row::create(int_entry('id', 2)),
                    Row::create(int_entry('id', 3)),
                ))->extract(new FlowContext($config))
            );

        $cache->expects(self::never())
            ->method('clear')
            ->with('id');

        $extractor = from_cache('id', from_rows($rowsToCache));

        \iterator_to_array($extractor->extract(new FlowContext($config)));

        self::assertEquals(
            [
                $rowsToCache,
            ],
            \iterator_to_array($extractor->extract(new FlowContext($config)))
        );
    }
}
