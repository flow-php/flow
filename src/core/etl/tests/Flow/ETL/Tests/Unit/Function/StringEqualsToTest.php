<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class StringEqualsToTest extends FlowTestCase
{
    public function test_equals_to_empty_strings() : void
    {
        self::assertTrue(
            ref('str')->stringEqualsTo('')->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_equals_to_exact_match() : void
    {
        self::assertTrue(
            ref('str')->stringEqualsTo('hello')->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_equals_to_no_match() : void
    {
        self::assertFalse(
            ref('str')->stringEqualsTo('world')->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_equals_to_null_comparison_string_returns_null() : void
    {
        self::assertNull(
            ref('str')->stringEqualsTo(ref('compare'))->eval(
                row(
                    str_entry('str', 'hello'),
                    str_entry('compare', null)
                )
            )
        );
    }

    public function test_equals_to_null_string_returns_null() : void
    {
        self::assertNull(
            ref('str')->stringEqualsTo('hello')->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_equals_to_with_scalar_function_parameter() : void
    {
        self::assertTrue(
            ref('str')->stringEqualsTo(ref('compare'))->eval(
                row(
                    str_entry('str', 'hello'),
                    str_entry('compare', 'hello')
                )
            )
        );
    }
}
