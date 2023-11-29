<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\all;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class AllTest extends TestCase
{
    public function test_all_expression_on_is_null_expression() : void
    {
        $this->assertTrue(
            all(ref('value')->isNull())->eval(Row::create(null_entry('value')))
        );
    }

    public function test_all_expression_on_multiple_boolean_values() : void
    {
        $this->assertTrue(
            all(lit(true), lit(true), lit(true))->eval(Row::create())
        );
    }

    public function test_all_expression_on_multiple_random_boolean_values() : void
    {
        $this->assertFalse(
            all(lit(true), lit(false), lit(true))->eval(Row::create())
        );
    }

    public function test_all_function_on_boolean_false_value() : void
    {
        $this->assertFalse(
            all(lit(false))->eval(Row::create())
        );
    }

    public function test_all_function_on_boolean_true_value() : void
    {
        $this->assertTrue(
            all(lit(true))->eval(Row::create())
        );
    }
}
