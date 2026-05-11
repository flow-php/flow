<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\any;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class AnyTest extends FlowTestCase
{
    public function test_any_expression_on_boolean_false_value(): void
    {
        static::assertFalse(any(lit(false))->eval(row(), flow_context()));
    }

    public function test_any_expression_on_boolean_true_value(): void
    {
        static::assertTrue(any(lit(true))->eval(row(), flow_context()));
    }

    public function test_any_expression_on_is_null_expression(): void
    {
        static::assertTrue(any(ref('value')->isNull())->eval(row(str_entry('value', null)), flow_context()));
    }

    public function test_any_expression_on_multiple_boolean_values(): void
    {
        static::assertTrue(any(lit(false), lit(true), lit(false))->eval(row(), flow_context()));
    }
}
