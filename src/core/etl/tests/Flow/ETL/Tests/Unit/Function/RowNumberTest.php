<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, ref, row_number, window};
use Flow\ETL\{Row, Rows};
use PHPUnit\Framework\TestCase;

final class RowNumberTest extends TestCase
{
    public function test_row_number_function_on_collection_of_rows_sorted_by_id_descending() : void
    {
        $rows = new Rows(
            $row1 = Row::create(int_entry('id', 1), int_entry('value', 1)),
            Row::create(int_entry('id', 2), int_entry('value', 1)),
            Row::create(int_entry('id', 3), int_entry('value', 1)),
            Row::create(int_entry('id', 4), int_entry('value', 1)),
            Row::create(int_entry('id', 5), int_entry('value', 1)),
        );

        $rowNumber = row_number()->over(window()->partitionBy(ref('value'))->orderBy(ref('id')->desc()));

        self::assertSame(5, $rowNumber->apply($row1, $rows));
    }
}
