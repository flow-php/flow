<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class StringEqualsToTest extends FlowTestCase
{
    public function test_equals_to_empty_strings(): void
    {
        static::assertTrue(ref('str')->stringEqualsTo('')->eval(row(['str' => '']), flow_context()));
    }

    public function test_equals_to_exact_match(): void
    {
        static::assertTrue(ref('str')->stringEqualsTo('hello')->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_equals_to_no_match(): void
    {
        static::assertFalse(ref('str')->stringEqualsTo('world')->eval(row(['str' => 'hello']), flow_context()));
    }

    public function test_equals_to_null_comparison_string_returns_null(): void
    {
        static::assertNull(
            ref('str')
                ->stringEqualsTo(ref('compare'))
                ->eval(row(['str' => 'hello', 'compare' => null]), flow_context()),
        );
    }

    public function test_equals_to_null_string_returns_null(): void
    {
        static::assertNull(ref('str')->stringEqualsTo('hello')->eval(row(['str' => null]), flow_context()));
    }

    public function test_equals_to_with_scalar_function_parameter(): void
    {
        static::assertTrue(
            ref('str')
                ->stringEqualsTo(ref('compare'))
                ->eval(row(['str' => 'hello', 'compare' => 'hello']), flow_context()),
        );
    }
}
