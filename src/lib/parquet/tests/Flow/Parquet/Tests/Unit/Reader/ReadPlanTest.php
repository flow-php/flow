<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Reader\RowSelection;
use PHPUnit\Framework\TestCase;

final class ReadPlanTest extends TestCase
{
    public function test_matched_rows_over_another_row_count_are_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A read plan selection and its matched rows must cover the same rows');

        new ReadPlan(RowSelection::all(3), RowSelection::all(2));
    }

    public function test_matched_rows_outside_the_selection_are_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A read plan can match only rows it selects');

        new ReadPlan(RowSelection::window(5, 2), RowSelection::all(5));
    }

    public function test_a_window_plan_reads_and_matches_the_window(): void
    {
        static::assertEquals(
            new ReadPlan(RowSelection::window(5, 1, 2), RowSelection::window(5, 1, 2)),
            ReadPlan::window(5, 1, 2),
        );
    }
}
