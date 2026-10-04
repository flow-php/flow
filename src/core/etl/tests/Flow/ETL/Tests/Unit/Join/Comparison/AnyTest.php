<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join\Comparison;

use Flow\ETL\Join\Comparison\Any;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Tests\Double\ConstantComparison;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class AnyTest extends FlowTestCase
{
    public function test_failure(): void
    {
        $comparison1 = new ConstantComparison(false);

        $comparison2 = new ConstantComparison(false);

        static::assertSame(
            [false],
            (new Any($comparison1, $comparison2))->compare(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['id' => 2]], schema(int_schema('id'))),
            ),
        );
    }

    public function test_success(): void
    {
        $comparison1 = new ConstantComparison(true);

        $comparison2 = new ConstantComparison(false);

        static::assertSame(
            [true],
            (new Any($comparison1, $comparison2))->compare(
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
