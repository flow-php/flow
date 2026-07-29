<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\window;

final class RowNumberTest extends FlowTestCase
{
    public function test_row_number_is_the_position_in_the_ordered_partition(): void
    {
        $rows = rows(
            row(int_entry('id', 5), int_entry('value', 1)),
            row(int_entry('id', 4), int_entry('value', 1)),
            row(int_entry('id', 3), int_entry('value', 1)),
            row(int_entry('id', 2), int_entry('value', 1)),
            $row1 = row(int_entry('id', 1), int_entry('value', 1)),
        );

        $rowNumber = row_number()->over(window()->partitionBy(ref('value'))->orderBy(ref('id')->desc()));

        static::assertSame(5, $rowNumber->apply(WindowContextMother::forRow($row1, $rows, 4)));
    }

    public function test_row_number_numbers_duplicated_rows_separately(): void
    {
        $rows = rows(
            $row1 = row(int_entry('id', 1), int_entry('value', 100)),
            $row2 = row(int_entry('id', 1), int_entry('value', 100)),
        );

        $rowNumber = row_number()->over(window()->orderBy(ref('value')));

        static::assertSame(1, $rowNumber->apply(WindowContextMother::forRow($row1, $rows, 0)));
        static::assertSame(2, $rowNumber->apply(WindowContextMother::forRow($row2, $rows, 1)));
    }
}
