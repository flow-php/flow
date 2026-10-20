<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class RowsExtractorTest extends FlowTestCase
{
    public function test_an_empty_source_refuses_to_infer_a_schema(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'from_rows() has nothing to infer a schema from: no row carries a column. Declare the schema with ->withSchema(...).',
        );

        from_rows()->schema();
    }

    public function test_an_empty_source_with_a_declared_schema_answers_it(): void
    {
        static::assertEquals(schema(int_schema('id')), from_rows()->withSchema(schema(int_schema('id')))->schema());
    }

    public function test_an_empty_source_extracts_nothing_without_a_declared_schema(): void
    {
        static::assertSame([], iterator_to_array(from_rows()->extract(flow_context()), false));
    }

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
