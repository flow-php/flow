<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Exception;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\string_schema;

final class InferredSchemaExceptionTest extends FlowTestCase
{
    public function test_columns_diverge_names_the_source_both_diffs_the_knobs_and_both_remedies(): void
    {
        $exception = InferredSchemaException::columnsDiverge(
            'file:///orders/part-02.csv',
            'file:///orders/part-01.csv',
            new Schema(int_schema('id'), string_schema('name'), string_schema('Zip')),
            ['id', 'name', 'w'],
            new SchemaInference(),
        );

        static::assertStringContainsString('file:///orders/part-02.csv', $exception->getMessage());
        static::assertStringContainsString('unexpected [w]', $exception->getMessage());
        static::assertStringContainsString('missing [Zip]', $exception->getMessage());
        static::assertStringContainsString('at most 20480 rows over at most 10 sources', $exception->getMessage());
        static::assertStringContainsString('file:///orders/part-01.csv', $exception->getMessage());
        static::assertStringContainsString('->withSchema(...)', $exception->getMessage());
        static::assertStringContainsString('infer_schema()->unionByName()', $exception->getMessage());
    }

    public function test_unbounded_knobs_are_spelled_all_in_the_prose(): void
    {
        $exception = InferredSchemaException::columnsDiverge(
            'file:///orders/part-02.csv',
            'file:///orders/part-01.csv',
            new Schema(int_schema('id')),
            [],
            new SchemaInference(-1, -1),
        );

        static::assertStringContainsString('at most all rows over at most all sources', $exception->getMessage());
        static::assertStringContainsString('unexpected [], missing [id]', $exception->getMessage());
    }
}
