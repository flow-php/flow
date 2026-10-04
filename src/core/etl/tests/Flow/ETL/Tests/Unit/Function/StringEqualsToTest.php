<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StringEqualsToTest extends FlowTestCase
{
    public function test_equals_to_empty_strings(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('str')->stringEqualsTo(''),
            ['str' => ''],
            schema(str_schema('str')),
        ));
    }

    public function test_equals_to_exact_match(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('str')->stringEqualsTo('hello'),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_equals_to_no_match(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('str')->stringEqualsTo('world'),
            ['str' => 'hello'],
            schema(str_schema('str')),
        ));
    }

    public function test_equals_to_null_comparison_string_returns_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->stringEqualsTo(ref('compare')),
            ['str' => 'hello', 'compare' => null],
            schema(str_schema('str'), str_schema('compare', nullable: true)),
        ));
    }

    public function test_equals_to_null_string_returns_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('str')->stringEqualsTo('hello'),
            ['str' => null],
            schema(str_schema('str', nullable: true)),
        ));
    }

    public function test_equals_to_with_scalar_function_parameter(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('str')->stringEqualsTo(ref('compare')),
            ['str' => 'hello', 'compare' => 'hello'],
            schema(str_schema('str'), str_schema('compare')),
        ));
    }
}
