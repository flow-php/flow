<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{any, lit, ref, str_entry};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class AnyTest extends TestCase
{
    public function test_any_expression_on_boolean_false_value() : void
    {
        self::assertFalse(
            any(lit(false))->eval(Row::create())
        );
    }

    public function test_any_expression_on_boolean_true_value() : void
    {
        self::assertTrue(
            any(lit(true))->eval(Row::create())
        );
    }

    public function test_any_expression_on_is_null_expression() : void
    {
        self::assertTrue(
            any(ref('value')->isNull())->eval(Row::create(str_entry('value', null)))
        );
    }

    public function test_any_expression_on_multiple_boolean_values() : void
    {
        self::assertTrue(
            any(lit(false), lit(true), lit(false))->eval(Row::create())
        );
    }
}
