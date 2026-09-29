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

final class StringBeforeLastTest extends FlowTestCase
{
    public function test_string_before_last(): void
    {
        static::assertSame('hello w', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringBeforeLast(ref('needle')),
            ['str' => 'hello world', 'needle' => 'o'],
            schema(str_schema('str'), str_schema('needle')),
        ));
    }

    public function test_string_before_last_including_needle(): void
    {
        static::assertSame('hello wo', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringBeforeLast(ref('needle'), includeNeedle: true),
            ['str' => 'hello world', 'needle' => 'o'],
            schema(str_schema('str'), str_schema('needle')),
        ));
    }

    public function test_string_before_last_returns_empty_string(): void
    {
        static::assertSame('', (new FunctionContext(flow_context()))->eval(
            ref('str')->stringBeforeLast(ref('needle')),
            ['str' => '', 'needle' => 'o'],
            schema(str_schema('str'), str_schema('needle')),
        ));
    }

    public function test_string_before_last_throws_on_null_input(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringBeforeLast function requires non-null value');

        (new FunctionContext(flow_context()))->eval(
            ref('str')->stringBeforeLast(ref('needle')),
            ['str' => null, 'needle' => 'o'],
            schema(str_schema('str', nullable: true), str_schema('needle')),
        );
    }
}
