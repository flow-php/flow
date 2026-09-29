<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class ReverseTest extends FlowTestCase
{
    public function test_reverse_ascii_string(): void
    {
        static::assertSame('olleh', (new FunctionContext(flow_context()))->eval(
            ref('str')->reverse(),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_reverse_empty_string(): void
    {
        static::assertSame('', (new FunctionContext(flow_context()))->eval(
            ref('str')->reverse(),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_reverse_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Reverse function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->reverse(),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }
}
