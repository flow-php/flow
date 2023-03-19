<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Join;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Expression;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;
use function Flow\ETL\DSL\col;

final class ExpressionTest extends TestCase
{
    public function test_expression() : void
    {
        $expression = Expression::on(new Equal('id', 'id'), '_');

        $this->assertSame('_', $expression->prefix());
        $this->assertEquals([col('id')], $expression->left());
        $this->assertEquals([col('id')], $expression->right());
    }

    public function test_expression_comparison() : void
    {
        $expression = Expression::on(new Equal('id', 'id'), '_');

        $this->assertTrue($expression->meet(
            Row::create(Entry::integer('id', 1)),
            Row::create(Entry::integer('id', 1)),
        ));
    }
}
