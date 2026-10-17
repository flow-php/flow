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

    public function test_a_split_is_one_selection_per_part_in_one_pass(): void
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
}
