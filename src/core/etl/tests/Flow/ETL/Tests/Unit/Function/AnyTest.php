<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\any;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class AnyTest extends TestCase
{
    public function test_any_expression_on_boolean_false_value() : void
    {
        $this->assertFalse(
            any(lit(false))->eval(Row::create())
        );
    }

    public function test_any_expression_on_boolean_true_value() : void
    {
        $this->assertTrue(
            any(lit(true))->eval(Row::create())
        );
    }

    public function test_any_expression_on_is_null_expression() : void
    {
        $this->assertTrue(
            any(ref('value')->isNull())->eval(Row::create(Entry::null('value')))
        );
    }

    public function test_any_expression_on_multiple_boolean_values() : void
    {
        $this->assertTrue(
            any(lit(false), lit(true), lit(false))->eval(Row::create())
        );
    }
}
