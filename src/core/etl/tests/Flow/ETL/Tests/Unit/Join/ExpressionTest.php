<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join;

use function Flow\ETL\DSL\{col, int_entry};
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Expression;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ExpressionTest extends TestCase
{
    public function test_expression() : void
    {
        $expression = Expression::on(new Equal('id', 'id'), '_');

        self::assertSame('_', $expression->prefix());
        self::assertEquals([col('id')], $expression->left());
        self::assertEquals([col('id')], $expression->right());
    }

    public function test_expression_comparison() : void
    {
        $expression = Expression::on(new Equal('id', 'id'), '_');

        self::assertTrue($expression->meet(
            Row::create(int_entry('id', 1)),
            Row::create(int_entry('id', 1)),
        ));
    }
}
