<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class AppendTest extends FlowTestCase
{
    public function test_append_empty_string_to_content(): void
    {
        static::assertEquals('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->append(''),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_append_to_empty_string(): void
    {
        static::assertEquals('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->append('hello'),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_append_to_non_empty_string(): void
    {
        static::assertEquals('hello world', (new FunctionContext(flow_context()))->eval(
            ref('str')->append(' world'),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_append_with_null_suffix(): void
    {
        static::assertEquals('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->append(ref('suffix')),
            ['str' => 'hello', 'suffix' => null],
            schema(str_schema('str'), str_schema('suffix', nullable: true)),
        ));
    }

    public function test_append_with_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Append function requires non-null value');

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->append(' world'),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        ));
    }

    public function test_append_with_null_value_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Append function requires non-null value');

        $context = flow_context(config());
        (new FunctionContext($context))->eval(
            ref('str')->append(' world'),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }

    public function test_append_with_scalar_function_parameter(): void
    {
        static::assertEquals('hello world', (new FunctionContext(flow_context()))->eval(
            ref('str')->append(ref('suffix')),
            ['str' => 'hello', 'suffix' => ' world'],
            schema(str_schema('str'), str_schema('suffix')),
        ));
    }
}
