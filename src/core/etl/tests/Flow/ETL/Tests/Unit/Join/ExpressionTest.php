<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join;

use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Expression;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\col;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class ExpressionTest extends FlowTestCase
{
    public function test_expression(): void
    {
        $expression = Expression::on(new Equal('id', 'id'), '_');

        static::assertSame('_', $expression->prefix());
        static::assertEquals([col('id')], $expression->left());
        static::assertEquals([col('id')], $expression->right());
    }

    public function test_expression_comparison(): void
    {
        $expression = Expression::on(new Equal('id', 'id'), '_');

        static::assertSame(
            [true],
            $expression->meet(
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
                array_to_rows([['id' => 1]], schema(int_schema('id'))),
            ),
        );
    }
}
