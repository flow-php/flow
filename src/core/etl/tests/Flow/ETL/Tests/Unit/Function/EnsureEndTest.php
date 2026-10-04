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

final class EnsureEndTest extends FlowTestCase
{
    public function test_empty_string_with_suffix(): void
    {
        static::assertEquals('_suffix', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureEnd('_suffix'),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_null_suffix(): void
    {
        static::assertEquals('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureEnd(ref('suffix')),
            ['str' => 'hello', 'suffix' => null],
            schema(str_schema('str'), str_schema('suffix', nullable: true)),
        ));
    }

    public function test_null_value(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('EnsureEnd function requires non-null value');

        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->ensureEnd('_suffix'),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        ));
    }

    public function test_string_already_ends_with_suffix(): void
    {
        static::assertEquals('document.txt', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureEnd('.txt'),
            ['str' => 'document.txt'],
            schema(str_schema('str')),
        ));
    }

    public function test_string_doesnt_end_with_suffix(): void
    {
        static::assertEquals('document.txt', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureEnd('.txt'),
            ['str' => 'document'],
            schema(str_schema('str')),
        ));
    }

    public function test_string_with_empty_suffix(): void
    {
        static::assertEquals('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureEnd(''),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_with_scalar_function_parameter(): void
    {
        static::assertEquals('document.pdf', (new FunctionContext(flow_context()))->eval(
            ref('str')->ensureEnd(ref('suffix')),
            ['str' => 'document', 'suffix' => '.pdf'],
            schema(str_schema('str'), str_schema('suffix')),
        ));
    }
}
