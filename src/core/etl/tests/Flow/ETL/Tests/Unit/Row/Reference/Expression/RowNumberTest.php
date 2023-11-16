<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\window;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class RowNumberTest extends TestCase
{
    public function test_row_number_function_on_collection_of_rows_sorted_by_id_descending() : void
    {
        $rows = new Rows(
            $row1 = Row::create(Entry::int('id', 1), Entry::int('value', 1)),
            Row::create(Entry::int('id', 2), Entry::int('value', 1)),
            Row::create(Entry::int('id', 3), Entry::int('value', 1)),
            Row::create(Entry::int('id', 4), Entry::int('value', 1)),
            Row::create(Entry::int('id', 5), Entry::int('value', 1)),
        );

        $rowNumber = row_number()->over(window()->partitionBy(ref('value'))->orderBy(ref('id')->desc()));

        $this->assertSame(5, $rowNumber->apply($row1, $rows));
    }
}
