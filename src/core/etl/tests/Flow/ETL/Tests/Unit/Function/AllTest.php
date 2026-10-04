<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Function\All;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Double\FailingOnValuesFunction;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\all;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
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
        static::assertSame($expected, (new FunctionContext(flow_context()))->eval(
            new All(lit($left), lit($right)),
            [],
            schema(),
        ));
    }

    public function test_all_expression_on_is_null_expression(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            all(ref('value')->isNull()),
            ['value' => null],
            schema(str_schema('value', nullable: true)),
        ));
    }

    public function test_all_expression_on_multiple_boolean_values(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            all(lit(true), lit(true), lit(true)),
            [],
            schema(),
        ));
    }

    public function test_all_expression_on_multiple_random_boolean_values(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            all(lit(true), lit(false), lit(true)),
            [],
            schema(),
        ));
    }

    public function test_all_function_on_boolean_false_value(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(all(lit(false)), [], schema()));
    }

    public function test_all_function_on_boolean_true_value(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(all(lit(true)), [], schema()));
    }

    public function test_an_undecided_row_only_reaches_the_next_argument(): void
    {
        $rows = array_to_rows(
            [['c' => false, 'v' => 99], ['c' => true, 'v' => 1]],
            schema(bool_schema('c'), int_schema('v')),
        );

        static::assertSame(
            [false, true],
            (new ReferenceResolver())
                ->resolve(all(ref('c'), (new FailingOnValuesFunction(ref('v'), [99]))->isNotNull()), $rows->schema())
                ->eval($rows, flow_context())
                ->values(),
        );
    }
}
