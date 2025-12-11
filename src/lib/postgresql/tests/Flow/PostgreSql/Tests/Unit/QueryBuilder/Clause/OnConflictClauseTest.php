<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\QueryBuilder\Clause\{ConflictAction, ConflictTarget, OnConflictClause};
use Flow\PostgreSql\QueryBuilder\Condition\{Comparison, ComparisonOperator};
use Flow\PostgreSql\QueryBuilder\Expression\{Column, FunctionCall, Literal};
use PHPUnit\Framework\TestCase;

final class OnConflictClauseTest extends TestCase
{
    public function test_do_nothing() : void
    {
        $conflict = OnConflictClause::doNothing();

        self::assertSame(ConflictAction::NOTHING, $conflict->action());
        self::assertNull($conflict->target());
        self::assertEmpty($conflict->updates());
    }

    public function test_do_nothing_with_target() : void
    {
        $target = ConflictTarget::columns(['email']);
        $conflict = OnConflictClause::doNothing($target);

        self::assertSame(ConflictAction::NOTHING, $conflict->action());
        self::assertNotNull($conflict->target());
    }

    public function test_do_update() : void
    {
        $target = ConflictTarget::columns(['email']);
        $conflict = OnConflictClause::doUpdate($target, [
            'name' => Column::name('excluded.name'),
            'updated_at' => new FunctionCall(['now']),
        ]);

        self::assertSame(ConflictAction::UPDATE, $conflict->action());
        self::assertNotNull($conflict->target());
        self::assertCount(2, $conflict->updates());
    }

    public function test_to_ast() : void
    {
        $target = ConflictTarget::columns(['email']);
        $conflict = OnConflictClause::doUpdate($target, [
            'name' => Column::name('excluded.name'),
        ]);
        $node = $conflict->toAst();
        $onConflictClause = $node->getOnConflictClause();

        self::assertNotNull($onConflictClause);
    }

    public function test_to_ast_and_from_ast() : void
    {
        $target = ConflictTarget::columns(['email']);
        $conflict = OnConflictClause::doUpdate($target, [
            'name' => Column::name('excluded.name'),
        ]);
        $node = $conflict->toAst();
        $restored = OnConflictClause::fromAst($node);

        self::assertSame($conflict->action(), $restored->action());
        self::assertCount(1, $restored->updates());
    }

    public function test_where() : void
    {
        $target = ConflictTarget::columns(['email']);
        $conflict = OnConflictClause::doUpdate($target, [
            'name' => Column::name('excluded.name'),
        ]);
        $condition = new Comparison(Column::name('deleted_at'), ComparisonOperator::EQ, Literal::null());
        $conflictWithWhere = $conflict->where($condition);

        self::assertNull($conflict->whereClause());
        self::assertNotNull($conflictWithWhere->whereClause());
    }
}
