<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor\File\InferredColumns;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class InferredColumnsTest extends FlowTestCase
{
    public function test_a_file_with_the_inferred_columns_in_any_order_passes(): void
    {
        $this->expectNotToPerformAssertions();

        (new InferredColumns(
            schema(int_schema('id'), str_schema('name')),
            ['_input_file_uri'],
            new SchemaInference(),
            'memory://a.csv',
        ))->refuseDivergence('memory://b.csv', ['name', 'id', 'id', '_input_file_uri']);
    }

    public function test_a_file_without_columns_passes(): void
    {
        $this->expectNotToPerformAssertions();

        (new InferredColumns(
            schema(int_schema('id'), str_schema('name')),
            ['_input_file_uri'],
            new SchemaInference(),
            'memory://a.csv',
        ))->refuseDivergence('memory://empty.csv', []);
    }

    public function test_a_file_whose_columns_diverge_is_refused_naming_both_sources(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'Columns of memory://b.csv do not match the schema inferred from memory://a.csv: unexpected [age], missing [name]',
        );

        (new InferredColumns(
            schema(int_schema('id'), str_schema('name')),
            ['_input_file_uri'],
            new SchemaInference(),
            'memory://a.csv',
        ))->refuseDivergence('memory://b.csv', ['id', 'age']);
    }

    public function test_normalize_round_trips_and_the_rebuilt_check_refuses_the_same_divergence(): void
    {
        $inferred = new InferredColumns(
            schema(int_schema('id')),
            ['year'],
            new SchemaInference(sampleSize: 5),
            'memory://a.csv',
        );

        static::assertSame('memory://a.csv', $inferred->normalize()['inferred_from']);
        static::assertSame(['year'], $inferred->normalize()['tail']);
        static::assertEquals($inferred, InferredColumns::fromArray($inferred->normalize()));

        $this->expectException(InferredSchemaException::class);

        InferredColumns::fromArray($inferred->normalize())->refuseDivergence('memory://b.csv', ['id', 'name']);
    }
}
