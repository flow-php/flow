<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\size;

final class SizeTest extends FlowTestCase
{
    public function test_size_expression_on_array_value(): void
    {
        static::assertSame(3, (new FunctionContext(flow_context()))->eval(
            size(lit(['foo', 'bar', 'baz'])),
            [],
            schema(),
        ));
    }

    public function test_size_expression_on_integer_value(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(size(lit(1)), [], schema()));
    }

    public function test_size_expression_on_string_value(): void
    {
        static::assertSame(3, (new FunctionContext(flow_context()))->eval(size(lit('foo')), [], schema()));
    }
}
