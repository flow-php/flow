<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;

final class EmptySourceTest extends FlowIntegrationTestCase
{
    public function test_from_all_without_extractors_refuses_to_infer(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage('from_all() has nothing to infer a schema from: it chains no extractor. Declare the schema with '
        . '->withSchema(...).');

        df()
            ->read(from_all())
            ->filter(ref('response_status_code')->equals(lit(200)))
            ->fetch();
    }

    public function test_from_all_without_extractors_reads_nothing_under_a_declared_schema(): void
    {
        static::assertCount(
            0,
            df()
                ->read(from_all()->withSchema(schema(int_schema('response_status_code'))))
                ->filter(ref('response_status_code')->equals(lit(200)))
                ->fetch(),
        );
    }

    public function test_from_all_over_an_empty_array_reads_nothing_under_a_declared_schema(): void
    {
        static::assertCount(
            0,
            df()
                ->read(from_all(from_array([]))->withSchema(schema(int_schema('response_status_code'))))
                ->filter(ref('response_status_code')->equals(lit(200)))
                ->fetch(),
        );
    }

    public function test_from_array_without_rows_refuses_to_infer(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'from_array() has nothing to infer a schema from: no row carries a column. Declare the schema with '
            . '->withSchema(...).',
        );

        df()->read(from_array([]))->withColumn('status', ref('response_status_code'))->fetch();
    }

    public function test_a_missing_column_in_non_empty_input_still_fails(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        df()
            ->read(from_array([['id' => 1]]))
            ->select('response_status_code')
            ->fetch();
    }
}
