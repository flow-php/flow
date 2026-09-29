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

final class StringBeforeTest extends FlowTestCase
{
    public function test_string_before(): void
    {
        static::assertSame('hello ', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringBefore(ref('needle')),
            ['str' => 'hello world', 'needle' => 'world'],
            schema(str_schema('str'), str_schema('needle')),
        ));

        static::assertSame('hell', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringBefore(ref('needle')),
            ['str' => 'hello world', 'needle' => 'o'],
            schema(str_schema('str'), str_schema('needle')),
        ));
    }

    public function test_string_before_including_needle(): void
    {
        static::assertSame('hello', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringBefore(ref('needle'), includeNeedle: true),
            ['str' => 'hello world', 'needle' => 'o'],
            schema(str_schema('str'), str_schema('needle')),
        ));
    }

    public function test_string_before_returns_empty_string(): void
    {
        static::assertSame('', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringBefore(ref('needle')),
            ['str' => '', 'needle' => 'o'],
            schema(str_schema('str'), str_schema('needle')),
        ));
    }

    public function test_string_before_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringBefore function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->stringBefore(ref('needle')),
            ['str' => null, 'needle' => 'o'],
            schema(str_schema('str', nullable: true), str_schema('needle')),
        );
    }
}
