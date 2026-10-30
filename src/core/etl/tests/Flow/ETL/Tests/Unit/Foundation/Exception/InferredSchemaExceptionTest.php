<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Exception;

use Flow\ETL\Foundation\Exception\ColumnMismatchException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

final class InferredSchemaExceptionTest extends FlowTestCase
{
    public function test_nothing_to_infer_from_names_the_source_the_reason_and_the_fix(): void
    {
        static::assertSame(
            'from_array() has nothing to infer a schema from: no row carries a column. Declare the schema with '
            . '->withSchema(...).',
            InferredSchemaException::nothingToInferFrom('from_array()', 'no row carries a column')->getMessage(),
        );
    }

    public function test_columns_diverge_names_the_source_both_diffs_the_knobs_and_both_remedies(): void
    {
        $exception = InferredSchemaException::columnsDiverge(
            'file:///orders/part-02.csv',
            'file:///orders/part-01.csv',
            ['id', 'name', 'Zip'],
            ['id', 'name', 'w'],
            20_480,
            10,
        );

        static::assertStringContainsString('file:///orders/part-02.csv', $exception->getMessage());
        static::assertStringContainsString('unexpected [w]', $exception->getMessage());
        static::assertStringContainsString('missing [Zip]', $exception->getMessage());
        static::assertStringContainsString('at most 20480 rows over at most 10 sources', $exception->getMessage());
        static::assertStringContainsString('file:///orders/part-01.csv', $exception->getMessage());
        static::assertStringContainsString('->withSchema(...)', $exception->getMessage());
        static::assertStringContainsString('infer_schema()->unionByName()', $exception->getMessage());
    }

    public function test_files_diverge_names_both_files_the_difference_and_the_remedy(): void
    {
        static::assertSame(
            "Columns of file:///orders/part-02.parquet do not match the schema read from file:///orders/part-01.parquet:\n"
            . "  Unexpected Definitions: \n"
            . "    |-- extra<string>\n"
            . 'Read the files as one wider schema with ->unionByName().',
            InferredSchemaException::filesDiverge(
                'file:///orders/part-02.parquet',
                'file:///orders/part-01.parquet',
                "  Unexpected Definitions: \n    |-- extra<string>\n",
            )->getMessage(),
        );
    }

    public function test_unbounded_knobs_are_spelled_all_in_the_prose(): void
    {
        $exception = InferredSchemaException::columnsDiverge(
            'file:///orders/part-02.csv',
            'file:///orders/part-01.csv',
            ['id'],
            [],
            -1,
            -1,
        );

        static::assertStringContainsString('at most all rows over at most all sources', $exception->getMessage());
        static::assertStringContainsString('unexpected [], missing [id]', $exception->getMessage());
    }

    public function test_a_row_past_the_sample_names_its_source_position_and_the_way_out(): void
    {
        $mismatch = new SchemaMismatchException(3, ColumnMismatchException::valueDoesNotMatch('code', 'integer', 'x'));
        $exception = InferredSchemaException::whenPastTheSample('from_array()', 2, $mismatch);

        static::assertInstanceOf(InferredSchemaException::class, $exception);
        static::assertSame(
            'Row 3 of from_array() does not fit the schema inferred from its first 2 rows: column "code": could not '
            . "convert 'x' (string) to integer. Infer from every row with ->inferSchema(infer_schema()->sampleSize(-1)), "
            . 'or declare the schema with ->withSchema(...).',
            $exception->getMessage(),
        );
        static::assertSame($mismatch, $exception->getPrevious());
    }

    /**
     * @param null|-1|positive-int $sampleSize null when the schema was declared, not inferred
     */
    #[TestWith([null])]
    #[TestWith([-1])]
    #[TestWith([5])]
    public function test_a_refusal_the_sample_does_not_explain_stays_the_gate_s_own(?int $sampleSize): void
    {
        $mismatch = new SchemaMismatchException(1, ColumnMismatchException::valueDoesNotMatch('code', 'integer', 'x'));

        static::assertSame($mismatch, InferredSchemaException::whenPastTheSample(
            'from_array()',
            $sampleSize,
            $mismatch,
        ));
    }
}
