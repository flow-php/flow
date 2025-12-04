<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Clause;

use Flow\PgQuery\QueryBuilder\Clause\ConflictTarget;
use Flow\PgQuery\QueryBuilder\Condition\{Comparison, ComparisonOperator};
use Flow\PgQuery\QueryBuilder\Expression\{Column, Literal};
use PHPUnit\Framework\TestCase;

final class ConflictTargetTest extends TestCase
{
    public function test_columns() : void
    {
        $target = ConflictTarget::columns(['email']);

        self::assertCount(1, $target->getColumns());
        self::assertSame(['email'], $target->getColumns());
        self::assertNull($target->getConstraint());
    }

    public function test_constraint() : void
    {
        $target = ConflictTarget::constraint('users_email_key');

        self::assertEmpty($target->getColumns());
        self::assertSame('users_email_key', $target->getConstraint());
    }

    public function test_multiple_columns() : void
    {
        $target = ConflictTarget::columns(['first_name', 'last_name']);

        self::assertCount(2, $target->getColumns());
        self::assertSame(['first_name', 'last_name'], $target->getColumns());
    }

    public function test_to_ast() : void
    {
        $target = ConflictTarget::columns(['email']);
        $node = $target->toAst();
        $inferClause = $node->getInferClause();

        self::assertNotNull($inferClause);
    }

    public function test_where() : void
    {
        $target = ConflictTarget::columns(['email']);
        $condition = new Comparison(Column::name('deleted_at'), ComparisonOperator::EQ, Literal::null());
        $targetWithWhere = $target->where($condition);

        self::assertNull($target->whereClause());
        self::assertNotNull($targetWithWhere->whereClause());
    }
}
