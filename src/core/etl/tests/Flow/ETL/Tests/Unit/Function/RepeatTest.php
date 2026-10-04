<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class RepeatTest extends FlowTestCase
{
    public function test_repeat_empty_string(): void
    {
        static::assertSame('', (new FunctionContext(flow_context()))->eval(
            ref('str')->repeat(3),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_repeat_negative_times(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Repeat function requires non-null, positive times');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->repeat(-1),
            ['str' => 'hello'],
            schema(str_schema('str')),
        );
    }

    public function test_repeat_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Repeat function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->repeat(3),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }

    public function test_repeat_string_multiple_times(): void
    {
        static::assertSame('hellohellohello', (new FunctionContext(flow_context()))->eval(
            ref('str')->repeat(3),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_repeat_with_null_times(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Repeat function requires non-null, positive times');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->repeat(ref('times')),
            ['str' => 'hello', 'times' => null],
            schema(str_schema('str'), str_schema('times', nullable: true)),
        );
    }

    public function test_repeat_with_scalar_function_times(): void
    {
        static::assertSame('hellohello', (new FunctionContext(flow_context()))->eval(
            ref('str')->repeat(ref('times')),
            ['str' => 'hello', 'times' => 2],
            schema(str_schema('str'), int_schema('times')),
        ));
    }

    public function test_repeat_zero_times(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Repeat function requires non-null, positive times');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->repeat(0),
            ['str' => 'hello'],
            schema(str_schema('str')),
        );
    }
}
