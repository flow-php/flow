<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\QueryBuilder\Clause\ConflictAction;
use Flow\PostgreSql\QueryBuilder\Clause\ConflictTarget;
use Flow\PostgreSql\QueryBuilder\Clause\OnConflictClause;
use Flow\PostgreSql\QueryBuilder\Condition\Comparison;
use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\FunctionCall;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use PHPUnit\Framework\TestCase;

final class OnConflictClauseTest extends TestCase
{
    public function test_do_nothing(): void
    {
        $conflict = OnConflictClause::doNothing();

        static::assertSame(ConflictAction::NOTHING, $conflict->action());
        static::assertNull($conflict->target());
        static::assertEmpty($conflict->updates());
    }

    public function test_do_nothing_with_target(): void
    {
        $target = ConflictTarget::columns(['email']);
        $conflict = OnConflictClause::doNothing($target);

        static::assertSame(ConflictAction::NOTHING, $conflict->action());
        static::assertNotNull($conflict->target());
    }

    public function test_do_update(): void
    {
        $target = ConflictTarget::columns(['email']);
        $conflict = OnConflictClause::doUpdate($target, [
            'name' => Column::name('excluded.name'),
            'updated_at' => new FunctionCall(['now']),
        ]);

        static::assertSame(ConflictAction::UPDATE, $conflict->action());
        static::assertNotNull($conflict->target());
        static::assertCount(2, $conflict->updates());
    }

    public function test_to_ast(): void
    {
        $target = ConflictTarget::columns(['email']);
        $conflict = OnConflictClause::doUpdate($target, [
            'name' => Column::name('excluded.name'),
        ]);
        $node = $conflict->toAst();
        $onConflictClause = $node->getOnConflictClause();

        static::assertNotNull($onConflictClause);
    }

    public function test_to_ast_and_from_ast(): void
    {
        $target = ConflictTarget::columns(['email']);
        $conflict = OnConflictClause::doUpdate($target, [
            'name' => Column::name('excluded.name'),
        ]);
        $node = $conflict->toAst();
        $restored = OnConflictClause::fromAst($node);

        static::assertSame($conflict->action(), $restored->action());
        static::assertCount(1, $restored->updates());
    }

    public function test_where(): void
    {
        $target = ConflictTarget::columns(['email']);
        $conflict = OnConflictClause::doUpdate($target, [
            'name' => Column::name('excluded.name'),
        ]);
        $condition = new Comparison(Column::name('deleted_at'), ComparisonOperator::EQ, Literal::null());
        $conflictWithWhere = $conflict->where($condition);

        static::assertNull($conflict->whereClause());
        static::assertNotNull($conflictWithWhere->whereClause());
    }
}
