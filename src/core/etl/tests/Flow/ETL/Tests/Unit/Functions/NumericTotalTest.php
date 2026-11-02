<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

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
