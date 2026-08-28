<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\Any;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\any;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class AnyTest extends FlowTestCase
{
    /**
     * @return \Generator<string, array{?bool, ?bool, ?bool}>
     */
    public static function three_valued_or(): Generator
    {
        yield 'true or false' => [true, false, true];

        yield 'true or null' => [true, null, true];

        yield 'false or false' => [false, false, false];

        yield 'false or null' => [false, null, null];

        yield 'null or null' => [null, null, null];
    }

    /**
     * @param ?bool $left
     * @param ?bool $right
     * @param ?bool $expected
     */
    #[DataProvider('three_valued_or')]
    public function test_three_valued_truth_table(?bool $left, ?bool $right, ?bool $expected): void
    {
        static::assertSame($expected, (new Any(lit($left), lit($right)))->eval(row(), flow_context()));
    }

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
