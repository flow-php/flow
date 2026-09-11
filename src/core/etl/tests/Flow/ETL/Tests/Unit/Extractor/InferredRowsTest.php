<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Extractor\InferredRows;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class InferredRowsTest extends FlowTestCase
{
    public function test_a_row_past_the_sample_is_reported_at_its_position_in_the_whole_source(): void
    {
        $rows = new InferredRows('from_array()', new SchemaInference(sampleSize: 2));
        $rows->of([['code' => 1], ['code' => 2]], schema(int_schema('code', true)), new PhpRowHydrator());

        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'Row 3 of from_array() does not fit the schema inferred from its first 2 rows: column "code": could not '
            . "convert 'x' (string) to integer. Infer from every row with ->inferSchema(infer_schema()->sampleSize(-1)), "
            . 'or declare the schema with ->withSchema(...).',
        );

        $rows->of([['code' => 3], ['code' => 'x']], schema(int_schema('code', true)), new PhpRowHydrator());
    }

    public function test_a_row_past_the_sample_keeps_the_refusal_it_was_raised_from(): void
    {
        try {
            (new InferredRows('from_array()', new SchemaInference(sampleSize: 1)))->of(
                [['code' => 1], ['code' => 'x']],
                schema(int_schema('code', true)),
                new PhpRowHydrator(),
            );

            static::fail('The row past the sample was not refused.');
        } catch (InferredSchemaException $exception) {
            static::assertInstanceOf(SchemaMismatchException::class, $exception->getPrevious());
        }
    }

    /**
     * @param null|-1|positive-int $sampleSize null when the schema was declared, not inferred
     */
    #[TestWith([null])]
    #[TestWith([-1])]
    #[TestWith([5])]
    public function test_a_refusal_the_sample_does_not_explain_stays_the_gate_s_own(?int $sampleSize): void
    {
        $this->expectException(SchemaMismatchException::class);
        $this->expectExceptionMessage('column "code" (row 1)');

        (new InferredRows(
            'from_array()',
            $sampleSize === null ? null : new SchemaInference(sampleSize: $sampleSize),
        ))->of([['code' => 1], ['code' => 'x']], schema(int_schema('code', true)), new PhpRowHydrator());
    }
}
