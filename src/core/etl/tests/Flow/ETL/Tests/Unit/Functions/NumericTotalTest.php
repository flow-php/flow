<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\VoidProgress;
use Flow\ETL\Functions\NumericTotal;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class NumericTotalTest extends FlowTestCase
{
    public function test_it_adds_up_and_counts_only_the_numeric_values(): void
    {
        $total = new NumericTotal();
        $total->aggregate(
            array_to_rows([
                ['v' => '1'],
                ['v' => 'x'],
                ['v' => null],
                ['v' => '2.5'],
            ], schema(str_schema('v', nullable: true)))->column('v'),
            [0, 1, 2, 3],
        );

        static::assertSame(3.5, $total->sum());
        static::assertSame(2, $total->count());
    }

    public function test_each_frame_totals_only_its_numeric_values(): void
    {
        static::assertSame(
            ['sums' => [3.5, 7.5, null, null], 'counts' => [2, 3, 0, 0]],
            NumericTotal::frames(
                ['1', 'x', null, '2.5', 4],
                [[0, 3], [0, 4], [1, 2], [1, 0]],
                false,
                new VoidProgress(),
            ),
        );
    }

    public function test_equal_frames_share_one_total(): void
    {
        static::assertSame(
            ['sums' => [9, 9, 5], 'counts' => [2, 2, 1]],
            NumericTotal::frames([4, 5], [[0, 1], [0, 1], [1, 1]], false, new VoidProgress()),
        );
    }

    public function test_frames_in_exact_mode_add_decimals_exactly(): void
    {
        static::assertSame(
            ['sums' => [0.1, 0.3], 'counts' => [1, 2]],
            NumericTotal::frames([0.1, 0.2], [[0, 0], [0, 1]], true, new VoidProgress()),
        );
    }

    public function test_a_single_row_is_added_without_taking_the_column(): void
    {
        $total = new NumericTotal();
        $total->aggregate(array_to_rows([['v' => 4], ['v' => 5]], schema(int_schema('v')))->column('v'), [1]);

        static::assertSame(5, $total->sum());
        static::assertSame(1, $total->count());
    }

    public function test_nothing_added_has_no_sum(): void
    {
        static::assertNull((new NumericTotal())->sum());
    }

    public function test_a_partial_merged_into_a_fresh_total_restores_it(): void
    {
        $total = new NumericTotal();
        $total->aggregate(array_to_rows([['v' => 4], ['v' => 5]], schema(int_schema('v')))->column('v'), [0, 1]);
        $restored = new NumericTotal();
        $restored->merge($total->partial());
        $restored->merge((new NumericTotal())->partial());

        static::assertSame(9, $restored->sum());
        static::assertSame(2, $restored->count());
    }
}
