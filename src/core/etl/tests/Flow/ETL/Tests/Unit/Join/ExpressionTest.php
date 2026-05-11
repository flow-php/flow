<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join;

use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Expression;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\col;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;

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

        static::assertTrue($expression->meet(row(int_entry('id', 1)), row(int_entry('id', 1))));
    }
}
