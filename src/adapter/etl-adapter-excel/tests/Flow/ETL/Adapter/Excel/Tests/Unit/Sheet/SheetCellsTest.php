<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\Sheet;

use Flow\ETL\Adapter\Excel\Sheet\SheetCells;
use Flow\ETL\Adapter\Excel\Tests\Double\FakeSheet;
use Flow\ETL\Tests\FlowTestCase;

use function iterator_to_array;

final class SheetCellsTest extends FlowTestCase
{
    public function test_a_sheet_with_no_rows_yields_nothing(): void
    {
        static::assertSame([], iterator_to_array((new SheetCells(new FakeSheet([]), true, 1))->rows(), false));
    }

    public function test_an_offset_past_the_last_row_yields_the_header_only(): void
    {
        static::assertSame(
            [['id', 'name']],
            iterator_to_array((new SheetCells(new FakeSheet([['id', 'name'], [1, 'a']]), true, 99))->rows(), false),
        );
    }

    public function test_offset_counts_from_the_header_row(): void
    {
        static::assertSame(
            [['id'], [3], [4]],
            iterator_to_array((new SheetCells(new FakeSheet([['id'], [1], [2], [3], [4]]), true, 4))->rows(), false),
        );
    }

    public function test_offset_without_a_header_skips_the_rows_before_it(): void
    {
        static::assertSame(
            [[3], [4]],
            iterator_to_array((new SheetCells(new FakeSheet([[1], [2], [3], [4]]), false, 3))->rows(), false),
        );
    }

    public function test_the_header_row_is_yielded_whatever_the_offset(): void
    {
        static::assertSame(
            ['id', 'name'],
            iterator_to_array((new SheetCells(new FakeSheet([['id', 'name'], [1, 'a']]), true, 2))->rows(), false)[0],
        );
    }

    public function test_widens_a_short_row_to_the_previous_width(): void
    {
        static::assertSame(
            [['a', 'b', 'c'], [1, 2, 3], [4, 5, null]],
            iterator_to_array(
                (new SheetCells(new FakeSheet([['a', 'b', 'c'], [1, 2, 3], [4, 5]]), true, 1))->rows(),
                false,
            ),
        );
    }

    public function test_a_row_wider_than_the_previous_one_is_not_truncated(): void
    {
        static::assertSame(
            [['a', 'b'], [1, 2], [3, 4, 5]],
            iterator_to_array((new SheetCells(new FakeSheet([['a', 'b'], [1, 2], [3, 4, 5]]), true, 1))->rows(), false),
        );
    }
}
