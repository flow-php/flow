<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_map;

final class RowSelectionTest extends TestCase
{
    /**
     * @param list<array{0: int, 1: bool}> $selectors
     */
    #[TestWith([0, null, [[10, false]]])]
    #[TestWith([3, null, [[3, true], [7, false]]])]
    #[TestWith([3, 4, [[3, true], [4, false], [3, true]]])]
    #[TestWith([0, 0, [[10, true]]])]
    #[TestWith([10, 5, [[10, true]]])]
    #[TestWith([12, null, [[10, true]]])]
    #[TestWith([8, 5, [[8, true], [2, false]]])]
    public function test_a_window_skips_the_offset_and_selects_up_to_the_limit(
        int $offset,
        ?int $limit,
        array $selectors,
    ): void {
        static::assertEquals(
            new RowSelection(...array_map(
                static fn(array $selector): RowSelector => new RowSelector(...$selector),
                $selectors,
            )),
            RowSelection::window(10, $offset, $limit),
        );
        static::assertSame(10, RowSelection::window(10, $offset, $limit)->totalRows());
    }

    public function test_adjacent_selectors_are_merged_and_empty_ones_dropped(): void
    {
        $selection = new RowSelection(
            new RowSelector(2, true),
            new RowSelector(0, false),
            new RowSelector(3, true),
            new RowSelector(1, false),
            new RowSelector(4, false),
        );

        static::assertEquals([new RowSelector(5, true), new RowSelector(5, false)], $selection->selectors());
    }

    public function test_the_intersection_selects_the_rows_both_select(): void
    {
        $left = new RowSelection(new RowSelector(4, false), new RowSelector(4, true), new RowSelector(2, false));
        $right = new RowSelection(new RowSelector(2, true), new RowSelector(7, false), new RowSelector(1, true));

        static::assertEquals(
            new RowSelection(
                new RowSelector(2, true),
                new RowSelector(2, false),
                new RowSelector(4, true),
                new RowSelector(1, false),
                new RowSelector(1, true),
            ),
            $left->intersection($right),
        );
        static::assertEquals($left->intersection($right), $right->intersection($left));
    }

    public function test_the_intersection_of_selections_of_no_row_selects_nothing(): void
    {
        static::assertEquals(new RowSelection(), RowSelection::all(0)->intersection(RowSelection::all(0)));
    }

    public function test_selections_of_other_row_counts_cannot_be_intersected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row selections of 10 and 9 rows cannot be combined');

        RowSelection::all(10)->intersection(RowSelection::all(9));
    }

    public function test_the_union_selects_the_rows_either_selects(): void
    {
        $left = new RowSelection(new RowSelector(4, false), new RowSelector(4, true), new RowSelector(2, false));
        $right = new RowSelection(new RowSelector(2, true), new RowSelector(3, false), new RowSelector(5, true));

        static::assertEquals(
            new RowSelection(new RowSelector(5, false), new RowSelector(3, true), new RowSelector(2, false)),
            $left->union($right),
        );
        static::assertEquals($left->union($right), $right->union($left));
        static::assertEquals(new RowSelection(), RowSelection::all(0)->union(RowSelection::none(0)));
    }

    public function test_the_difference_selects_the_rows_only_the_first_selects(): void
    {
        $left = new RowSelection(new RowSelector(4, false), new RowSelector(4, true), new RowSelector(2, false));
        $right = new RowSelection(new RowSelector(2, true), new RowSelector(7, false), new RowSelector(1, true));

        static::assertEquals(
            new RowSelection(new RowSelector(2, false), new RowSelector(7, true), new RowSelector(1, false)),
            $left->difference($right),
        );
        static::assertEquals(RowSelection::none(10), $left->difference($left));
    }

    public function test_a_mask_narrows_the_selected_rows_in_order(): void
    {
        $selection = new RowSelection(
            new RowSelector(2, true),
            new RowSelector(4, false),
            new RowSelector(3, true),
            new RowSelector(2, false),
        );

        static::assertEquals(
            new RowSelection(
                new RowSelector(3, true),
                new RowSelector(2, false),
                new RowSelector(4, true),
                new RowSelector(1, false),
                new RowSelector(1, true),
            ),
            $selection->masked([false, true, true, false, true, false]),
        );
        static::assertEquals($selection, $selection->masked([true, true, true, true, true, true]));
        static::assertEquals(RowSelection::none(11), $selection->masked([false, false, false, false, false, false]));
    }

    public function test_the_head_selects_the_first_selected_rows(): void
    {
        $selection = new RowSelection(
            new RowSelector(2, true),
            new RowSelector(3, false),
            new RowSelector(1, true),
            new RowSelector(4, false),
        );

        static::assertEquals(
            new RowSelection(
                new RowSelector(2, true),
                new RowSelector(3, false),
                new RowSelector(1, true),
                new RowSelector(1, false),
                new RowSelector(3, true),
            ),
            $selection->head(4),
        );
        static::assertEquals($selection, $selection->head(7));
        static::assertEquals($selection, $selection->head(9));
        static::assertEquals(RowSelection::none(10), $selection->head(0));
    }

    public function test_a_mask_of_another_row_count_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A mask of 2 rows cannot narrow a selection of 3 rows');

        RowSelection::window(5, 1, 3)->masked([true, false]);
    }

    public function test_selections_of_other_row_counts_cannot_be_united(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Row selections of 10 and 9 rows cannot be combined');

        RowSelection::all(10)->union(RowSelection::all(9));
    }

    public function test_none_skips_every_row(): void
    {
        static::assertEquals(new RowSelection(new RowSelector(4, true)), RowSelection::none(4));
        static::assertSame(4, RowSelection::none(4)->totalRows());
        static::assertSame(0, RowSelection::none(4)->rowCount());
    }

    public function test_a_selection_contains_every_selection_of_a_subset_of_its_rows(): void
    {
        $selection = new RowSelection(new RowSelector(2, true), new RowSelector(5, false), new RowSelector(2, true));

        static::assertTrue($selection->contains($selection));
        static::assertTrue($selection->contains(RowSelection::none(9)));
        static::assertTrue($selection->contains(RowSelection::window(9, 3, 2)));
        static::assertFalse($selection->contains(RowSelection::window(9, 1, 2)));
        static::assertFalse($selection->contains(RowSelection::all(9)));
    }

    public function test_the_row_count_is_the_selected_rows_and_selects_any_tells_whether_there_are_some(): void
    {
        $selection = new RowSelection(new RowSelector(2, true), new RowSelector(3, false), new RowSelector(4, true));

        static::assertSame(3, $selection->rowCount());
        static::assertSame(9, $selection->totalRows());
        static::assertTrue($selection->selectsAny());
        static::assertFalse(RowSelection::none(9)->selectsAny());
        static::assertFalse(RowSelection::all(0)->selectsAny());
    }

    public function test_a_split_is_one_selection_per_part(): void
    {
        $selection = new RowSelection(new RowSelector(2, true), new RowSelector(3, false), new RowSelector(4, true));

        static::assertEquals(
            [
                new RowSelection(new RowSelector(1, true)),
                new RowSelection(new RowSelector(1, true), new RowSelector(2, false)),
                new RowSelection(),
                new RowSelection(new RowSelector(1, false), new RowSelector(4, true)),
            ],
            $selection->split([1, 3, 0, 5]),
        );
        static::assertEquals([new RowSelection()], RowSelection::all(0)->split([0]));
        static::assertEquals([], RowSelection::all(0)->split([]));
    }

    /**
     * @param list<int> $rowCounts
     */
    #[TestWith([[4, 4]])]
    #[TestWith([[5, 5]])]
    #[TestWith([[]])]
    public function test_a_split_into_parts_of_another_row_count_is_refused(array $rowCounts): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A selection of 9 rows cannot be split into parts of');

        RowSelection::all(9)->split($rowCounts);
    }

    public function test_a_slice_is_the_rows_from_an_offset_its_first_row_being_row_0(): void
    {
        $selection = new RowSelection(
            new RowSelector(2, true),
            new RowSelector(3, false),
            new RowSelector(4, true),
            new RowSelector(1, false),
        );

        static::assertEquals(
            new RowSelection(new RowSelector(1, true), new RowSelector(2, false)),
            $selection->slice(1, 3),
        );
        static::assertEquals(new RowSelection(new RowSelector(2, false)), $selection->slice(3, 2));
        static::assertEquals(
            new RowSelection(new RowSelector(1, false), new RowSelector(4, true), new RowSelector(1, false)),
            $selection->slice(4, 6),
        );
        static::assertEquals(new RowSelection(), $selection->slice(10, 0));
        static::assertEquals($selection, $selection->slice(0, 10));
        static::assertEquals(new RowSelection(), RowSelection::all(0)->slice(0, 0));
    }

    #[TestWith([-1, 1])]
    #[TestWith([0, -1])]
    #[TestWith([8, 3])]
    public function test_a_slice_outside_the_selection_is_refused(int $offset, int $rows): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('lie outside a selection of 10 rows');

        RowSelection::all(10)->slice($offset, $rows);
    }
}
