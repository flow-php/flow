<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\All;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\all;
use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class AllTest extends FlowTestCase
{
    /**
     * @return \Generator<string, array{?bool, ?bool, ?bool}>
     */
    public static function three_valued_and(): Generator
    {
        yield 'true and true' => [true, true, true];

        yield 'true and false' => [true, false, false];

        yield 'true and null' => [true, null, null];

        yield 'false and null' => [false, null, false];

        yield 'null and null' => [null, null, null];
    }

    /**
     * @param ?bool $left
     * @param ?bool $right
     * @param ?bool $expected
     */
    #[DataProvider('three_valued_and')]
    public function test_three_valued_truth_table(?bool $left, ?bool $right, ?bool $expected): void
    {
        static::assertSame($expected, (new All(lit($left), lit($right)))->eval(
            array_to_row([], schema()),
            flow_context(),
        ));
    }

    public function test_all_expression_on_is_null_expression(): void
    {
        static::assertTrue(
            all(ref('value')->isNull())
                ->eval(array_to_row(['value' => null], schema(str_schema('value', nullable: true))), flow_context()),
        );
    }

    public function test_all_expression_on_multiple_boolean_values(): void
    {
        static::assertTrue(all(lit(true), lit(true), lit(true))->eval(array_to_row([], schema()), flow_context()));
    }

    public function test_all_expression_on_multiple_random_boolean_values(): void
    {
        static::assertFalse(all(lit(true), lit(false), lit(true))->eval(array_to_row([], schema()), flow_context()));
    }

    public function test_all_function_on_boolean_false_value(): void
    {
        static::assertFalse(all(lit(false))->eval(array_to_row([], schema()), flow_context()));
    }

    public function test_all_function_on_boolean_true_value(): void
    {
        static::assertTrue(all(lit(true))->eval(array_to_row([], schema()), flow_context()));
    }
}
