<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\window;

final class RowNumberTest extends FlowTestCase
{
    public function test_row_number_is_the_position_in_the_ordered_partition(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 5, 'value' => 1],
                ['id' => 4, 'value' => 1],
                ['id' => 3, 'value' => 1],
                ['id' => 2, 'value' => 1],
                ['id' => 1, 'value' => 1],
            ],
            schema(int_schema('id'), int_schema('value')),
        );

        $rowNumber = row_number()->over(window()->partitionBy(ref('value'))->orderBy(ref('id')->desc()));

        static::assertSame(5, $rowNumber->apply(WindowContextMother::atIndex($rows, 4)));
    }

    public function test_row_number_numbers_duplicated_rows_separately(): void
    {
        $rows = array_to_rows(
            [['id' => 1, 'value' => 100], ['id' => 1, 'value' => 100]],
            schema(int_schema('id'), int_schema('value')),
        );

        $rowNumber = row_number()->over(window()->orderBy(ref('value')));

        static::assertSame(1, $rowNumber->apply(WindowContextMother::atIndex($rows, 0)));
        static::assertSame(2, $rowNumber->apply(WindowContextMother::atIndex($rows, 1)));
    }

    public function test_with_children_returns_the_same_leaf(): void
    {
        $function = row_number();

        static::assertSame([], $function->children());
        static::assertSame($function, $function->withChildren([]));
    }
}
