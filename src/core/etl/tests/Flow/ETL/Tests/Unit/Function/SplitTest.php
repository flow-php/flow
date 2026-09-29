<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\split;

final class SplitTest extends FlowTestCase
{
    public function test_split_not_string(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        (new FunctionContext(flow_context()))->eval(split(lit(123), ','), [], schema());
    }

    public function test_split_string(): void
    {
        static::assertSame(
            ['foo', 'bar', 'baz'],
            (new FunctionContext(flow_context()))->eval(split(lit('foo,bar,baz'), ','), [], schema()),
        );
    }

    public function test_split_string_with_limit(): void
    {
        static::assertSame(
            ['foo', 'bar,baz'],
            (new FunctionContext(flow_context()))->eval(split(lit('foo,bar,baz'), ',', 2), [], schema()),
        );
    }
}
