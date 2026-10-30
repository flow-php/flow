<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions\Join\Condition;

use Flow\ETL\Functions\Join\Condition\Any;
use Flow\ETL\Functions\Join\Condition\Equal;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Double\ConstantComparison;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class AnyTest extends FlowTestCase
{
    public function test_describe_round_trips(): void
    {
        $comparison = new Any(new Equal('a', 'a'), new Equal('b', 'b'));
        $left = array_to_rows(
            [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y'], ['a' => 3, 'b' => 'z']],
            schema(int_schema('a'), str_schema('b')),
        );
        $right = array_to_rows(
            [['a' => 1, 'b' => 'x'], ['a' => 9, 'b' => 'y'], ['a' => 3, 'b' => 'q']],
            schema(int_schema('a'), str_schema('b')),
        );

        static::assertSame(
            $comparison->compare($left, $right),
            DescribeRoundTrip::rebuilt($comparison)->compare($left, $right),
        );
    }

    public function test_failure(): void
    {
        static::assertSame(
            [false],
            (new Any(new ConstantComparison(false), new ConstantComparison(false)))->compare(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['id' => 2]], schema(int_schema('id'))),
            ),
        );
    }

    public function test_success(): void
    {
        static::assertSame(
            [true],
            (new Any(new ConstantComparison(true), new ConstantComparison(false)))->compare(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['id' => 2]], schema(int_schema('id'))),
            ),
        );
    }

    public function test_combines_children_element_wise(): void
    {
        static::assertSame(
            [true, true, false],
            (new Any(new Equal('a', 'a'), new Equal('b', 'b')))->compare(
                array_to_rows(
                    [['a' => 1, 'b' => 1], ['a' => 1, 'b' => 2], ['a' => 3, 'b' => 3]],
                    schema(int_schema('a'), int_schema('b')),
                ),
                array_to_rows(
                    [['a' => 1, 'b' => 1], ['a' => 1, 'b' => 9], ['a' => 9, 'b' => 9]],
                    schema(int_schema('a'), int_schema('b')),
                ),
            ),
        );
    }
}
