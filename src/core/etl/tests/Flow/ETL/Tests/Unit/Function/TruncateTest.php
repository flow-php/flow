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

final class TruncateTest extends FlowTestCase
{
    public function test_truncate_ellipsis_longer_than_limit(): void
    {
        static::assertSame('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(5, 'verylongellipsis'),
            ['str' => 'hello world'],
            schema(str_schema('str')),
        ));
    }

    public function test_truncate_empty_string(): void
    {
        static::assertSame('', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(10),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_truncate_exact_length(): void
    {
        static::assertSame('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(5),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_truncate_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Truncate function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(10),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        );
    }

    public function test_truncate_string_longer_than_limit(): void
    {
        static::assertSame('he...', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(5),
            ['str' => 'hello world'],
            schema(str_schema('str')),
        ));
    }

    public function test_truncate_string_shorter_than_limit(): void
    {
        static::assertSame('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(10),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_truncate_with_custom_ellipsis(): void
    {
        static::assertSame('he***', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(5, '***'),
            ['str' => 'hello world'],
            schema(str_schema('str')),
        ));
    }

    public function test_truncate_with_length_one(): void
    {
        static::assertSame('h', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(1),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_truncate_with_length_zero(): void
    {
        static::assertSame('', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(0),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_truncate_with_negative_length(): void
    {
        static::assertSame('hell', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(-1),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_truncate_with_null_length(): void
    {
        static::assertSame('hello world', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(ref('length')),
            ['str' => 'hello world', 'length' => null],
            schema(str_schema('str'), str_schema('length', nullable: true)),
        ));
    }

    public function test_truncate_with_scalar_function_ellipsis(): void
    {
        static::assertSame('hel>>', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(5, ref('ellipsis')),
            ['str' => 'hello world', 'ellipsis' => '>>'],
            schema(str_schema('str'), str_schema('ellipsis')),
        ));
    }

    public function test_truncate_with_scalar_function_length(): void
    {
        static::assertSame('he...', (new FunctionContext(flow_context()))->eval(
            ref('str')->truncate(ref('length')),
            ['str' => 'hello world', 'length' => 5],
            schema(str_schema('str'), int_schema('length')),
        ));
    }
}
