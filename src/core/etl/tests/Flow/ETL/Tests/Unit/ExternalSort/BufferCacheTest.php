<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\ExternalSort;

use Flow\ETL\Cache;
use Flow\ETL\DSL\Entry;
use Flow\ETL\ExternalSort\BufferCache;
use Flow\ETL\Row;
use Flow\ETL\Rows;
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

        $cacheMock->expects($this->once())
            ->method('add')
            ->with('id', new Callback(fn (Rows $rows) => $rows->count() === 2));

        $bufferCache->add('id', new Rows(Row::create(Entry::integer('id', 1))));
        $bufferCache->add('id', new Rows(Row::create(Entry::integer('id', 2))));

        $bufferCache->close();
    }

    public function test_buffer_cache_overflow() : void
    {
        $bufferCache = new BufferCache(
            $cacheMock = $this->createMock(Cache::class),
            2,
        );

        $cacheMock->expects($this->once())
            ->method('add')
            ->with('id', new Callback(fn (Rows $rows) => $rows->count() === 2));

        $bufferCache->add('id', new Rows(Row::create(Entry::integer('id', 1))));
        $bufferCache->add('id', new Rows(Row::create(Entry::integer('id', 2))));
    }

    public function test_buffer_cache_overflow_and_close() : void
    {
        $bufferCache = new BufferCache(
            $cacheMock = $this->createMock(Cache::class),
            2,
        );

        $cacheMock->expects($this->exactly(2))
            ->method('add');

        $bufferCache->add('id', new Rows(Row::create(Entry::integer('id', 1))));
        $bufferCache->add('id', new Rows(Row::create(Entry::integer('id', 2))));
        $bufferCache->add('id', new Rows(Row::create(Entry::integer('id', 3))));

        $bufferCache->close();
    }
}
