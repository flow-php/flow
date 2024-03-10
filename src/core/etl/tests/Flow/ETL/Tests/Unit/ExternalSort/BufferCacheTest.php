<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\ExternalSort;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\ExternalSort\BufferCache;
use Flow\ETL\{Cache, Row, Rows};
use PHPUnit\Framework\Constraint\Callback;
use PHPUnit\Framework\TestCase;

final class BufferCacheTest extends TestCase
{
    public function test_buffer_cache_close() : void
    {
        $bufferCache = new BufferCache(
            $cacheMock = $this->createMock(Cache::class),
            10,
        );

        $cacheMock->expects(self::once())
            ->method('add')
            ->with('id', new Callback(fn (Rows $rows) => $rows->count() === 2));

        $bufferCache->add('id', new Rows(Row::create(int_entry('id', 1))));
        $bufferCache->add('id', new Rows(Row::create(int_entry('id', 2))));

        $bufferCache->close();
    }

    public function test_buffer_cache_overflow() : void
    {
        $bufferCache = new BufferCache(
            $cacheMock = $this->createMock(Cache::class),
            2,
        );

        $cacheMock->expects(self::once())
            ->method('add')
            ->with('id', new Callback(fn (Rows $rows) => $rows->count() === 2));

        $bufferCache->add('id', new Rows(Row::create(int_entry('id', 1))));
        $bufferCache->add('id', new Rows(Row::create(int_entry('id', 2))));
    }

    public function test_buffer_cache_overflow_and_close() : void
    {
        $bufferCache = new BufferCache(
            $cacheMock = $this->createMock(Cache::class),
            2,
        );

        $cacheMock->expects(self::exactly(2))
            ->method('add');

        $bufferCache->add('id', new Rows(Row::create(int_entry('id', 1))));
        $bufferCache->add('id', new Rows(Row::create(int_entry('id', 2))));
        $bufferCache->add('id', new Rows(Row::create(int_entry('id', 3))));

        $bufferCache->close();
    }
}
