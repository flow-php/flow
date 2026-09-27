<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class StringEqualsToTest extends FlowTestCase
{
    public function test_equals_to_empty_strings(): void
    {
        static::assertTrue(
            ref('str')
                ->stringEqualsTo('')
                ->eval(array_to_row(['str' => ''], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_equals_to_exact_match(): void
    {
        static::assertTrue(
            ref('str')
                ->stringEqualsTo('hello')
                ->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_equals_to_no_match(): void
    {
        static::assertFalse(
            ref('str')
                ->stringEqualsTo('world')
                ->eval(array_to_row(['str' => 'hello'], schema(str_schema('str'))), flow_context()),
        );
    }

    public function test_equals_to_null_comparison_string_returns_null(): void
    {
        static::assertNull(
            ref('str')
                ->stringEqualsTo(ref('compare'))
                ->eval(
                    array_to_row(
                        ['str' => 'hello', 'compare' => null],
                        schema(str_schema('str'), str_schema('compare', nullable: true)),
                    ),
                    flow_context(),
                ),
        );
    }

    public function test_equals_to_null_string_returns_null(): void
    {
        static::assertNull(
            ref('str')
                ->stringEqualsTo('hello')
                ->eval(array_to_row(['str' => null], schema(str_schema('str', nullable: true))), flow_context()),
        );
    }

    public function test_equals_to_with_scalar_function_parameter(): void
    {
        static::assertTrue(
            ref('str')
                ->stringEqualsTo(ref('compare'))
                ->eval(
                    array_to_row(
                        ['str' => 'hello', 'compare' => 'hello'],
                        schema(str_schema('str'), str_schema('compare')),
                    ),
                    flow_context(),
                ),
        );
    }
}
