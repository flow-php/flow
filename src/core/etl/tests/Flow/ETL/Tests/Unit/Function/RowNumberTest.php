<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\window;

final class RowNumberTest extends FlowTestCase
{
    public function test_row_number_function_on_collection_of_rows_sorted_by_id_descending(): void
    {
        $rows = rows(
            $row1 = row(int_entry('id', 1), int_entry('value', 1)),
            row(int_entry('id', 2), int_entry('value', 1)),
            row(int_entry('id', 3), int_entry('value', 1)),
            row(int_entry('id', 4), int_entry('value', 1)),
            row(int_entry('id', 5), int_entry('value', 1)),
        );

        $rowNumber = row_number()->over(window()->partitionBy(ref('value'))->orderBy(ref('id')->desc()));

        static::assertSame(5, $rowNumber->apply($row1, $rows, flow_context()));
    }
}
