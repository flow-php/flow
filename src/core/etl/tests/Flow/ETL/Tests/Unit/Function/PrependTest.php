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

final class PrependTest extends FlowTestCase
{
    public function test_prepend_empty_string_to_content(): void
    {
        static::assertEquals('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->prepend(''),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_prepend_to_empty_string(): void
    {
        static::assertEquals('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->prepend('hello'),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_prepend_to_non_empty_string(): void
    {
        static::assertEquals('Hello world', (new FunctionContext(flow_context()))->eval(
            ref('str')->prepend('Hello '),
            ['str' => 'world'],
            schema(str_schema('str')),
        ));
    }

    public function test_prepend_with_null_prefix(): void
    {
        static::assertEquals('world', (new FunctionContext(flow_context()))->eval(
            ref('str')->prepend(ref('prefix')),
            ['str' => 'world', 'prefix' => null],
            schema(str_schema('str'), str_schema('prefix', nullable: true)),
        ));
    }

    public function test_prepend_with_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Prepend function requires non-null value');

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->prepend('Hello '),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        ));
    }

    public function test_prepend_with_scalar_function_parameter(): void
    {
        static::assertEquals('Hello world', (new FunctionContext(flow_context()))->eval(
            ref('str')->prepend(ref('prefix')),
            ['str' => 'world', 'prefix' => 'Hello '],
            schema(str_schema('str'), str_schema('prefix')),
        ));
    }
}
