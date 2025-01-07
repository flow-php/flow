<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use function Flow\ETL\DSL\{from_rows, int_entry, str_entry};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\{Tests\FlowTestCase};

final class RowsExtractorTest extends FlowTestCase
{
    public function test_process_extractor() : void
    {
        $rows = rows(row(int_entry('number', 1), str_entry('name', 'one')), row(int_entry('number', 2), str_entry('name', 'two')), row(int_entry('number', 3), str_entry('name', 'tree')), row(int_entry('number', 4), str_entry('name', 'four')), row(int_entry('number', 5), str_entry('name', 'five')));

        $extractor = from_rows($rows);

        self::assertExtractedRowsAsArrayEquals(
            [
                ['number' => 1, 'name' => 'one'],
                ['number' => 2, 'name' => 'two'],
                ['number' => 3, 'name' => 'tree'],
                ['number' => 4, 'name' => 'four'],
                ['number' => 5, 'name' => 'five'],
            ],
            $extractor
        );
    }
}
