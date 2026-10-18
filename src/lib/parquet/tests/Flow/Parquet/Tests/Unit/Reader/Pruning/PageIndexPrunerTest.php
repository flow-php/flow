<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader\Pruning;

use Flow\Parquet\Reader\Pruning\PrunedRows;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;
use Flow\Parquet\Tests\Context\OneGroupPagePruning;
use Flow\Parquet\Tests\Double\InMemoryIndexes;
use Flow\Parquet\Tests\Mother\PageIndexMother;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;

final class PageIndexPrunerTest extends TestCase
{
    public function test_columns_whose_pages_break_at_other_rows_are_evaluated_over_the_rows_their_pages_share(): void
    {
        static::assertEquals(
            new PrunedRows(
                new RowSelection(new RowSelector(5, true), new RowSelector(2, false), new RowSelector(3, true)),
                new RowSelection(new RowSelector(5, true), new RowSelector(2, false), new RowSelector(3, true)),
            ),
            OneGroupPagePruning::pruned(
                parquet_column('a')->greaterThanOrEqual(5)->and(parquet_column('b')->lessThan(107)),
                new InMemoryIndexes([
                    'a' => [PageIndexMother::int64([[0, 4], [5, 9]])],
                    'b' => [PageIndexMother::int64([[100, 102], [103, 106], [107, 109]])],
                ], ['a' => [PageIndexMother::pagesAt(0, 5)], 'b' => [PageIndexMother::pagesAt(0, 3, 7)]]),
            ),
        );
    }

    public function test_only_the_rows_of_pages_that_always_match_are_matched(): void
    {
        static::assertEquals(
            new PrunedRows(
                new RowSelection(new RowSelector(5, true), new RowSelector(5, false)),
                new RowSelection(new RowSelector(7, true), new RowSelector(3, false)),
            ),
            OneGroupPagePruning::pruned(
                parquet_column('a')->greaterThanOrEqual(5),
                new InMemoryIndexes(['a' => [PageIndexMother::int64([[0, 4], [5, 6], [7, 9]], [
                    0,
                    1,
                    0,
                ])]], ['a' => [PageIndexMother::pagesAt(0, 5, 7)]]),
            ),
        );
    }

    public function test_a_null_page_never_matches_a_comparison(): void
    {
        static::assertEquals(
            new PrunedRows(
                new RowSelection(new RowSelector(5, false), new RowSelector(5, true)),
                RowSelection::none(10),
            ),
            OneGroupPagePruning::pruned(
                parquet_column('a')->lessThan(100),
                new InMemoryIndexes(['a' => [PageIndexMother::int64([
                    [0, 200],
                    null,
                ])]], ['a' => [PageIndexMother::pagesAt(0, 5)]]),
            ),
        );
    }

    public function test_a_filtered_column_without_an_index_keeps_the_whole_group(): void
    {
        $rows = new PrunedRows(RowSelection::window(10, 1), RowSelection::none(10));

        static::assertEquals($rows, OneGroupPagePruning::pruned(
            parquet_column('a')->greaterThanOrEqual(5)->and(parquet_column('b')->lessThan(107)),
            new InMemoryIndexes(['a' => [PageIndexMother::int64([[0, 4], [5, 9]])]], [
                'a' => [PageIndexMother::pagesAt(0, 5)],
                'b' => [PageIndexMother::pagesAt(0, 3, 7)],
            ]),
            $rows,
        ));
    }

    public function test_the_pruned_rows_stay_inside_the_rows_given(): void
    {
        static::assertEquals(
            new PrunedRows(
                new RowSelection(new RowSelector(5, true), new RowSelector(3, false), new RowSelector(2, true)),
                new RowSelection(new RowSelector(5, true), new RowSelector(3, false), new RowSelector(2, true)),
            ),
            OneGroupPagePruning::pruned(
                parquet_column('a')->greaterThanOrEqual(5),
                new InMemoryIndexes(['a' => [PageIndexMother::int64([
                    [0, 4],
                    [5, 9],
                ])]], ['a' => [PageIndexMother::pagesAt(0, 5)]]),
                new PrunedRows(RowSelection::window(10, 3, 5), RowSelection::none(10)),
            ),
        );
    }

    public function test_a_group_without_a_selected_row_reads_no_index(): void
    {
        $indexes = new InMemoryIndexes();

        OneGroupPagePruning::pruned(
            parquet_column('a')->greaterThanOrEqual(5),
            $indexes,
            new PrunedRows(RowSelection::none(10), RowSelection::none(10)),
        );

        static::assertSame([], $indexes->reads);
    }

    public function test_a_group_whose_selected_rows_are_all_matched_reads_no_index(): void
    {
        $indexes = new InMemoryIndexes(['a' => [PageIndexMother::int64([
            [0, 4],
            [5, 9],
        ])]], ['a' => [PageIndexMother::pagesAt(0, 5)]]);
        $rows = new PrunedRows(RowSelection::window(10, 2), RowSelection::window(10, 2));

        static::assertEquals($rows, OneGroupPagePruning::pruned(
            parquet_column('a')->greaterThanOrEqual(5),
            $indexes,
            $rows,
        ));
        static::assertSame([], $indexes->reads);
    }

    public function test_a_column_index_and_offset_index_of_different_page_counts_keep_the_whole_group(): void
    {
        $rows = new PrunedRows(RowSelection::all(10), RowSelection::none(10));

        static::assertEquals($rows, OneGroupPagePruning::pruned(
            parquet_column('a')->greaterThanOrEqual(5),
            new InMemoryIndexes(['a' => [PageIndexMother::int64([[0, 4], [5, 9]])]], ['a' => [PageIndexMother::pagesAt(
                0,
                3,
                5,
            )]]),
            $rows,
        ));
    }

    public function test_an_offset_index_out_of_row_order_keeps_the_whole_group(): void
    {
        $rows = new PrunedRows(RowSelection::all(10), RowSelection::none(10));

        static::assertEquals($rows, OneGroupPagePruning::pruned(
            parquet_column('a')->greaterThanOrEqual(5),
            new InMemoryIndexes(['a' => [PageIndexMother::int64([
                [0, 4],
                [5, 9],
                [0, 0],
            ])]], ['a' => [PageIndexMother::pagesAt(0, 5, 3)]]),
            $rows,
        ));
    }
}
