<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\size;

final class SizeTest extends FlowTestCase
{
    public function test_size_expression_on_array_value(): void
    {
        static::assertSame(3, size(lit(['foo', 'bar', 'baz']))->eval(array_to_row([], schema()), flow_context()));
    }

    public function test_size_expression_on_integer_value(): void
    {
        static::assertNull(size(lit(1))->eval(array_to_row([], schema()), flow_context()));
    }

    public function test_size_expression_on_string_value(): void
    {
        static::assertSame(3, size(lit('foo'))->eval(array_to_row([], schema()), flow_context()));
    }
}
