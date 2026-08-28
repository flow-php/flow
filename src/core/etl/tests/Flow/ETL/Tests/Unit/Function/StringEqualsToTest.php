<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class StringEqualsToTest extends FlowTestCase
{
    public function test_equals_to_empty_strings(): void
    {
        static::assertTrue(ref('str')->stringEqualsTo('')->eval(row(str_entry('str', '')), flow_context()));
    }

    public function test_equals_to_exact_match(): void
    {
        static::assertTrue(ref('str')->stringEqualsTo('hello')->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_equals_to_no_match(): void
    {
        static::assertFalse(ref('str')->stringEqualsTo('world')->eval(row(str_entry('str', 'hello')), flow_context()));
    }

    public function test_equals_to_null_comparison_string_returns_null(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringEqualsTo function requires non-null string');

        ref('str')
            ->stringEqualsTo(ref('compare'))
            ->eval(row(str_entry('str', 'hello'), str_entry('compare', null)), flow_context());
    }

    public function test_equals_to_null_string_returns_null(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('StringEqualsTo function requires non-null value');

        ref('str')->stringEqualsTo('hello')->eval(row(str_entry('str', null)), flow_context());
    }

    public function test_equals_to_with_scalar_function_parameter(): void
    {
        static::assertTrue(
            ref('str')
                ->stringEqualsTo(ref('compare'))
                ->eval(row(str_entry('str', 'hello'), str_entry('compare', 'hello')), flow_context()),
        );
    }
}
