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

    public function test_extractor_with_a_reason_names_it_in_the_message(): void
    {
        static::assertSame(
            'PostgreSqlCursorExtractor cannot describe what it will produce before producing it: column "location" '
            . 'has PostgreSQL type "point", which Flow has no type for. Declare the schema with ->withSchema().',
            SchemaNotDerivableException::extractor(
                'PostgreSqlCursorExtractor',
                'column "location" has PostgreSQL type "point", which Flow has no type for',
            )->getMessage(),
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
            'ChainExtractor cannot read its dataset twice, so discover_pivot_values() cannot scan it before the '
            . 'pivot runs. Declare the values with pivot_values(...).',
            SchemaNotDerivableException::nonRewindable('ChainExtractor')->getMessage(),
        );
    }
}
