<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader\Pruning;

use Flow\Parquet\Reader\Pruning\PrunedRows;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Reader\RowSelector;
use Flow\Parquet\Tests\Context\ThreeRowGroupsPruning;
use Flow\Parquet\Tests\Double\RowGroupChunksSpy;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;

final class RowGroupStatisticsPrunerTest extends TestCase
{
    public function test_a_group_that_never_matches_is_skipped_and_one_that_always_matches_is_matched(): void
    {
        static::assertEquals(
            new PrunedRows(
                new RowSelection(new RowSelector(2, true), new RowSelector(4, false)),
                new RowSelection(new RowSelector(4, true), new RowSelector(2, false)),
            ),
            ThreeRowGroupsPruning::pruned(parquet_column('id')->greaterThan(3), RowSelection::all(6)),
        );
    }

    public function test_the_pruned_rows_stay_inside_the_rows_given(): void
    {
        static::assertEquals(
            new PrunedRows(
                new RowSelection(new RowSelector(2, true), new RowSelector(1, false), new RowSelector(3, true)),
                new RowSelection(new RowSelector(6, true)),
            ),
            ThreeRowGroupsPruning::pruned(parquet_column('id')->greaterThan(3), RowSelection::window(6, 1, 2)),
        );
    }

    public function test_the_rows_matched_before_stay_matched_inside_the_selection(): void
    {
        static::assertEquals(
            new PrunedRows(
                new RowSelection(new RowSelector(2, true), new RowSelector(4, false)),
                new RowSelection(
                    new RowSelector(2, true),
                    new RowSelector(1, false),
                    new RowSelector(1, true),
                    new RowSelector(2, false),
                ),
            ),
            ThreeRowGroupsPruning::pruned(
                parquet_column('id')->greaterThan(3),
                RowSelection::all(6),
                new RowSelection(new RowSelector(3, false), new RowSelector(3, true)),
            ),
        );
    }

    public function test_a_group_without_a_selected_row_is_not_evaluated(): void
    {
        $chunks = new RowGroupChunksSpy(ThreeRowGroupsPruning::chunks());

        ThreeRowGroupsPruning::pruned(
            parquet_column('id')->greaterThan(3),
            RowSelection::window(6, 2, 2),
            null,
            $chunks,
        );

        static::assertSame([1], $chunks->chunkReads);
    }
}
