<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\window;

final class RowNumberTest extends FlowTestCase
{
    public function test_row_number_is_the_position_in_the_ordered_partition(): void
    {
        $rows = rows(
            schema(int_schema('id'), int_schema('value')),
            row(['id' => 5, 'value' => 1]),
            row(['id' => 4, 'value' => 1]),
            row(['id' => 3, 'value' => 1]),
            row(['id' => 2, 'value' => 1]),
            $row1 = row(['id' => 1, 'value' => 1]),
        );

        $rowNumber = row_number()->over(window()->partitionBy(ref('value'))->orderBy(ref('id')->desc()));

        static::assertSame(5, $rowNumber->apply(WindowContextMother::forRow($row1, $rows, 4)));
    }

    public function test_row_number_numbers_duplicated_rows_separately(): void
    {
        $rows = rows(
            schema(int_schema('id'), int_schema('value')),
            $row1 = row(['id' => 1, 'value' => 100]),
            $row2 = row(['id' => 1, 'value' => 100]),
        );

        $rowNumber = row_number()->over(window()->orderBy(ref('value')));

        static::assertSame(1, $rowNumber->apply(WindowContextMother::forRow($row1, $rows, 0)));
        static::assertSame(2, $rowNumber->apply(WindowContextMother::forRow($row2, $rows, 1)));
    }

    public function test_with_children_returns_the_same_leaf(): void
    {
        $function = row_number();

        static::assertSame([], $function->children());
        static::assertSame($function, $function->withChildren([]));
    }
}
