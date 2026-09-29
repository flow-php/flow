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

final class EnsureStartTest extends FlowTestCase
{
    public function test_empty_string_with_prefix(): void
    {
        static::assertEquals('prefix_', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureStart('prefix_'),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_null_prefix(): void
    {
        static::assertEquals('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureStart(ref('prefix')),
            ['str' => 'hello', 'prefix' => null],
            schema(str_schema('str'), str_schema('prefix', nullable: true)),
        ));
    }

    public function test_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('EnsureStart function requires non-null value');

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->ensureStart('prefix_'),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        ));
    }

    public function test_string_already_starts_with_prefix(): void
    {
        static::assertEquals('https://example.com', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureStart('https://'),
            ['str' => 'https://example.com'],
            schema(str_schema('str')),
        ));
    }

    public function test_string_doesnt_start_with_prefix(): void
    {
        static::assertEquals('https://example.com', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureStart('https://'),
            ['str' => 'example.com'],
            schema(str_schema('str')),
        ));
    }

    public function test_string_with_empty_prefix(): void
    {
        static::assertEquals('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureStart(''),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_with_scalar_function_parameter(): void
    {
        static::assertEquals('https://example.com', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureStart(ref('prefix')),
            ['str' => 'example.com', 'prefix' => 'https://'],
            schema(str_schema('str'), str_schema('prefix')),
        ));
    }
}
