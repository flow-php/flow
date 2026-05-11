<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\all;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class AllTest extends FlowTestCase
{
    public function test_all_expression_on_is_null_expression(): void
    {
        static::assertTrue(all(ref('value')->isNull())->eval(row(str_entry('value', null)), flow_context()));
    }

    public function test_all_expression_on_multiple_boolean_values(): void
    {
        static::assertTrue(all(lit(true), lit(true), lit(true))->eval(row(), flow_context()));
    }

    public function test_all_expression_on_multiple_random_boolean_values(): void
    {
        static::assertFalse(all(lit(true), lit(false), lit(true))->eval(row(), flow_context()));
    }

    public function test_all_function_on_boolean_false_value(): void
    {
        static::assertFalse(all(lit(false))->eval(row(), flow_context()));
    }

    public function test_all_function_on_boolean_true_value(): void
    {
        static::assertTrue(all(lit(true))->eval(row(), flow_context()));
    }
}
