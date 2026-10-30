<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\HashJoin;

use Flow\ETL\Kernels\HashJoin\RowMerger;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowMergerTest extends FlowTestCase
{
    public function test_columns_are_gathered_by_side_in_the_output_order_and_names(): void
    {
        $merged = (new RowMerger(
            schema(int_schema('amount'), int_schema('r_id'), str_schema('r_name')),
            ['amount'],
            ['id', 'name'],
        ))->merge(
            array_to_rows([['id' => 1, 'amount' => 100]], schema(int_schema('id'), int_schema('amount'))),
            array_to_rows([['id' => 2, 'name' => 'Alice']], schema(int_schema('id'), str_schema('name'))),
        );

        static::assertSame([['amount' => 100, 'r_id' => 2, 'r_name' => 'Alice']], $merged->toArray());
        static::assertEquals(schema(int_schema('amount'), int_schema('r_id'), str_schema('r_name')), $merged->schema());
    }

    public function test_a_column_named_on_both_sides_comes_from_the_side_it_is_listed_for(): void
    {
        static::assertSame(
            [['country' => 'PL', 'id' => 2]],
            (new RowMerger(schema(str_schema('country'), int_schema('id')), ['country'], ['id']))
                ->merge(
                    array_to_rows([['id' => 1, 'country' => 'PL']], schema(int_schema('id'), str_schema('country'))),
                    array_to_rows([['id' => 2]], schema(int_schema('id'))),
                )
                ->toArray(),
        );
    }

    public function test_a_column_a_batch_lacks_is_left_out(): void
    {
        static::assertSame(
            [['id' => 1]],
            (new RowMerger(schema(int_schema('id'), str_schema('name')), ['id'], ['name']))
                ->merge(
                    array_to_rows([['id' => 1]], schema(int_schema('id'))),
                    array_to_rows([['other' => 'x']], schema(str_schema('other'))),
                )
                ->toArray(),
        );
    }
}
