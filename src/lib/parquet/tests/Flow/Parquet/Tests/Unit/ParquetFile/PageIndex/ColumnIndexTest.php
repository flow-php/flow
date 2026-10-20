<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\PageIndex;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\PageIndex\BoundaryOrder;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use PHPUnit\Framework\TestCase;

final class ColumnIndexTest extends TestCase
{
    public function test_it_round_trips_through_thrift_with_null_and_nan_counts(): void
    {
        $index = new ColumnIndex([false, true], ['a', ''], ['c', ''], BoundaryOrder::ASCENDING, [0, 3], [1, 0]);

        static::assertEquals($index, ColumnIndex::fromThrift($index->toThrift()));
        static::assertSame(2, $index->pages());
    }

    public function test_counts_a_writer_left_out_stay_unknown(): void
    {
        $index = new ColumnIndex([false], ['a'], ['c'], BoundaryOrder::UNORDERED);

        static::assertEquals($index, ColumnIndex::fromThrift($index->toThrift()));
        static::assertNull(ColumnIndex::fromThrift($index->toThrift())->nullCounts);
    }

    public function test_lists_of_another_length_than_the_pages_are_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Every list of a column index has one entry per page');

        new ColumnIndex([false, false], ['a', 'b'], ['c', 'd'], BoundaryOrder::UNORDERED, null, [0]);
    }
}
