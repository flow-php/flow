<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\QueryBuilder\Clause\ConflictTarget;
use Flow\PostgreSql\QueryBuilder\Condition\Comparison;
use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use PHPUnit\Framework\TestCase;

final class ConflictTargetTest extends TestCase
{
    public function test_columns(): void
    {
        $target = ConflictTarget::columns(['email']);

        static::assertCount(1, $target->getColumns());
        static::assertSame(['email'], $target->getColumns());
        static::assertNull($target->getConstraint());
    }

    public function test_constraint(): void
    {
        $target = ConflictTarget::constraint('users_email_key');

        static::assertEmpty($target->getColumns());
        static::assertSame('users_email_key', $target->getConstraint());
    }

    public function test_multiple_columns(): void
    {
        $target = ConflictTarget::columns(['first_name', 'last_name']);

        static::assertCount(2, $target->getColumns());
        static::assertSame(['first_name', 'last_name'], $target->getColumns());
    }

    public function test_to_ast(): void
    {
        $target = ConflictTarget::columns(['email']);
        $node = $target->toAst();
        $inferClause = $node->getInferClause();

        static::assertNotNull($inferClause);
    }

    public function test_where(): void
    {
        $target = ConflictTarget::columns(['email']);
        $condition = new Comparison(Column::name('deleted_at'), ComparisonOperator::EQ, Literal::null());
        $targetWithWhere = $target->where($condition);

        static::assertNull($target->whereClause());
        static::assertNotNull($targetWithWhere->whereClause());
    }
}
