<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\IO\Extractor\Sheet;

use Flow\ETL\Adapter\Excel\IO\Extractor\Sheet\SheetCells;
use Flow\ETL\Adapter\Excel\Tests\Double\FakeSheet;
use Flow\ETL\Tests\FlowTestCase;

use function iterator_to_array;

final class SheetCellsTest extends FlowTestCase
{
    public function test_a_sheet_with_no_rows_yields_nothing(): void
    {
        static::assertSame([], iterator_to_array((new SheetCells(new FakeSheet([]), true))->rows(), false));
    }

    public function test_widens_a_short_row_to_the_previous_width(): void
    {
        static::assertSame(
            [['a', 'b', 'c'], [1, 2, 3], [4, 5, null]],
            iterator_to_array(
                (new SheetCells(new FakeSheet([['a', 'b', 'c'], [1, 2, 3], [4, 5]]), true))->rows(),
                false,
            ),
        );
    }

    public function test_a_row_wider_than_the_previous_one_is_not_truncated(): void
    {
        static::assertSame(
            [['a', 'b'], [1, 2], [3, 4, 5]],
            iterator_to_array((new SheetCells(new FakeSheet([['a', 'b'], [1, 2], [3, 4, 5]]), true))->rows(), false),
        );
    }
}
