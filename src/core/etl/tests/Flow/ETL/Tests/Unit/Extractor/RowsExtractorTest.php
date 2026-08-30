<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowsExtractorTest extends FlowTestCase
{
    public function test_process_extractor(): void
    {
        $rows = rows(
            schema(int_schema('number'), str_schema('name')),
            row(['number' => 1, 'name' => 'one']),
            row(['number' => 2, 'name' => 'two']),
            row(['number' => 3, 'name' => 'tree']),
            row(['number' => 4, 'name' => 'four']),
            row(['number' => 5, 'name' => 'five']),
        );

        $extractor = from_rows($rows);

        self::assertExtractedRowsAsArrayEquals(
            [
                ['number' => 1, 'name' => 'one'],
                ['number' => 2, 'name' => 'two'],
                ['number' => 3, 'name' => 'tree'],
                ['number' => 4, 'name' => 'four'],
                ['number' => 5, 'name' => 'five'],
            ],
            $extractor,
        );
    }
}
