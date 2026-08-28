<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Exception;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Tests\FlowTestCase;

final class SchemaNotDerivableExceptionTest extends FlowTestCase
{
    public function test_extractor_names_the_extractor_and_the_way_out(): void
    {
        static::assertSame(
            'CallbackExtractor cannot describe what it will produce before producing it. Declare the schema with '
            . '->withSchema(), or read from a source that describes itself.',
            SchemaNotDerivableException::extractor('CallbackExtractor')->getMessage(),
        );
    }

    public function test_function_names_the_function_and_the_reason(): void
    {
        static::assertSame(
            'array_get() cannot describe the column it produces: the array operand declares "list<string>", which is not a structure.',
            SchemaNotDerivableException::function(
                'array_get',
                'the array operand declares "list<string>", which is not a structure',
            )->getMessage(),
        );
    }

    public function test_non_rewindable_names_the_extractor_and_the_way_out(): void
    {
        static::assertSame(
            'ChainExtractor cannot read its dataset twice, so describing it would consume the rows before they '
            . 'are extracted. Pass an array, or declare the schema with ->withSchema().',
            SchemaNotDerivableException::nonRewindable('ChainExtractor')->getMessage(),
        );
    }

    public function test_pipeline_names_the_extractor(): void
    {
        static::assertSame(
            'DataFrameExtractor holds a whole pipeline, so its output columns depend on every step in it. Reading '
            . 'the schema would have to run the pipeline, which is what asking before extraction avoids.',
            SchemaNotDerivableException::pipeline('DataFrameExtractor')->getMessage(),
        );
    }
}
