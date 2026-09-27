<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RowsExtractorTest extends FlowTestCase
{
    public function test_process_extractor(): void
    {
        $rows = array_to_rows(
            [
                ['number' => 1, 'name' => 'one'],
                ['number' => 2, 'name' => 'two'],
                ['number' => 3, 'name' => 'tree'],
                ['number' => 4, 'name' => 'four'],
                ['number' => 5, 'name' => 'five'],
            ],
            schema(int_schema('number'), str_schema('name')),
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

    public function test_extract_yields_batches_matching_schema(): void
    {
        $extractor = from_rows(
            array_to_rows([['number' => 1]], schema(int_schema('number'))),
            array_to_rows([['number' => 2, 'name' => 'two']], schema(int_schema('number'), str_schema('name'))),
        );

        foreach ($extractor->extract(flow_context()) as $batch) {
            static::assertTrue($batch->schema()->isSame($extractor->schema()));
        }
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(from_rows(array_to_rows([['number' => 1]], schema(int_schema('number'))))->isRepeatable());
    }

    public function test_it_declares_an_exact_row_count(): void
    {
        $extractor = from_rows(
            array_to_rows([['number' => 1], ['number' => 2]], schema(int_schema('number'))),
            array_to_rows([['number' => 3]], schema(int_schema('number'))),
        );

        static::assertEquals(new Statistics(rows: Cardinality::exact(3)), $extractor->statistics());
        static::assertSame($extractor->statistics(), $extractor->statistics());
    }
}
