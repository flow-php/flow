<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{all, flow_context, lit, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class AllTest extends FlowTestCase
{
    public function test_all_expression_on_is_null_expression() : void
    {
        self::assertTrue(
            all(ref('value')->isNull())->eval(row(str_entry('value', null)), flow_context())
        );
    }

    public function test_all_expression_on_multiple_boolean_values() : void
    {
        self::assertTrue(
            all(lit(true), lit(true), lit(true))->eval(row(), flow_context())
        );
    }

    public function test_all_expression_on_multiple_random_boolean_values() : void
    {
        self::assertFalse(
            all(lit(true), lit(false), lit(true))->eval(row(), flow_context())
        );
    }

    public function test_all_function_on_boolean_false_value() : void
    {
        self::assertFalse(
            all(lit(false))->eval(row(), flow_context())
        );
    }

    public function test_all_function_on_boolean_true_value() : void
    {
        self::assertTrue(
            all(lit(true))->eval(row(), flow_context())
        );
    }
}
