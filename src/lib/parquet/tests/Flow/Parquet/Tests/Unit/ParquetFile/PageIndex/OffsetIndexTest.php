<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\PageIndex;

use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\PageIndex\PageLocation;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Mother\ColumnChunkMother;
use Flow\Parquet\Tests\Mother\PageIndexMother;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class OffsetIndexTest extends TestCase
{
    public function test_it_round_trips_through_thrift(): void
    {
        $index = new OffsetIndex([new PageLocation(4, 30, 0), new PageLocation(34, 25, 7)]);

        static::assertEquals($index, OffsetIndex::fromThrift($index->toThrift()));
    }

    #[TestWith([0, 0])]
    #[TestWith([3, 0])]
    #[TestWith([4, 1])]
    #[TestWith([6, 1])]
    #[TestWith([7, 2])]
    #[TestWith([20, 2])]
    public function test_the_page_of_a_row_is_the_last_page_starting_at_or_before_it(int $row, int $page): void
    {
        static::assertSame($page, PageIndexMother::pagesAt(0, 4, 7)->pageOf($row));
    }

    public function test_a_page_ends_where_the_next_begins_and_the_last_at_the_row_group_end(): void
    {
        $index = PageIndexMother::pagesAt(0, 4, 7);

        static::assertSame([4, 3, 3], [$index->pageRows(0, 10), $index->pageRows(1, 10), $index->pageRows(2, 10)]);
    }

    /**
     * @param list<int> $firstRows
     */
    #[TestWith([[0, 4, 7], true])]
    #[TestWith([[0], true])]
    #[TestWith([[], false])]
    #[TestWith([[1, 4], false])]
    #[TestWith([[0, 5, 3], false])]
    #[TestWith([[0, 4, 4], false])]
    #[TestWith([[0, 10], false])]
    public function test_pages_fit_a_row_group_when_they_start_at_row_zero_and_increase_inside_it(
        array $firstRows,
        bool $fits,
    ): void {
        static::assertSame($fits, PageIndexMother::pagesAt(...$firstRows)->fits(
            ColumnChunkMother::spanning(FlatColumn::int64('id'), 100, 30),
            10,
        ));
    }

    /**
     * @param list<array{int, int}> $pages offset and size of pages starting at rows 0, 5, ...
     */
    #[TestWith([[[100, 10], [110, 20]], true])]
    #[TestWith([[[99, 10], [110, 10]], false])]
    #[TestWith([[[100, 10], [125, 10]], false])]
    #[TestWith([[[100, 0], [110, 10]], false])]
    #[TestWith([[[110, 10], [100, 10]], false])]
    #[TestWith([[[100, 15], [110, 10]], false])]
    public function test_pages_fit_a_chunk_when_they_lie_inside_it_one_after_another(array $pages, bool $fits): void
    {
        $locations = [];

        foreach ($pages as $page => [$offset, $size]) {
            $locations[] = new PageLocation($offset, $size, $page * 5);
        }

        static::assertSame($fits, (new OffsetIndex($locations))->fits(
            ColumnChunkMother::spanning(FlatColumn::int64('id'), 100, 30),
            10,
        ));
    }
}
