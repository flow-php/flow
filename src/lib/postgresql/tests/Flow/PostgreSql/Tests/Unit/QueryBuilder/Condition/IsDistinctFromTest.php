<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\QueryBuilder\Condition\{AndCondition, IsDistinctFrom, NotCondition, OrCondition};
use Flow\PostgreSql\QueryBuilder\Expression\{Column, Literal};
use PHPUnit\Framework\TestCase;

final class IsDistinctFromTest extends TestCase
{
    public function test_and_method_returns_and_condition() : void
    {
        $condition1 = new IsDistinctFrom(Column::name('status'), Literal::null(), false);
        $condition2 = new IsDistinctFrom(Column::name('value'), Literal::int(0), false);

        $result = $condition1->and($condition2);

        self::assertInstanceOf(AndCondition::class, $result);
    }

    public function test_converts_is_distinct_from_to_ast() : void
    {
        $condition = new IsDistinctFrom(
            Column::name('status'),
            Literal::null(),
            false
        );

        $node = $condition->toAst();

        self::assertTrue($node->hasAExpr());

        $aExpr = $node->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_DISTINCT, $aExpr->getKind());
        self::assertTrue($aExpr->hasLexpr());
        self::assertTrue($aExpr->hasRexpr());
    }

    public function test_converts_is_not_distinct_from_to_ast() : void
    {
        $condition = new IsDistinctFrom(
            Column::name('status'),
            Literal::null(),
            true
        );

        $node = $condition->toAst();

        self::assertTrue($node->hasAExpr());

        $aExpr = $node->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_NOT_DISTINCT, $aExpr->getKind());
    }

    public function test_creates_is_distinct_from_condition() : void
    {
        $condition = new IsDistinctFrom(
            Column::name('status'),
            Literal::null(),
            false
        );

        self::assertInstanceOf(IsDistinctFrom::class, $condition);
        self::assertFalse($condition->negated);
    }

    public function test_creates_is_not_distinct_from_condition() : void
    {
        $condition = new IsDistinctFrom(
            Column::name('status'),
            Literal::null(),
            true
        );

        self::assertInstanceOf(IsDistinctFrom::class, $condition);
        self::assertTrue($condition->negated);
    }

    public function test_not_method_returns_not_condition() : void
    {
        $condition = new IsDistinctFrom(Column::name('status'), Literal::null(), false);

        $result = $condition->not();

        self::assertInstanceOf(NotCondition::class, $result);
    }

    public function test_or_method_returns_or_condition() : void
    {
        $condition1 = new IsDistinctFrom(Column::name('status'), Literal::null(), false);
        $condition2 = new IsDistinctFrom(Column::name('value'), Literal::int(0), false);

        $result = $condition1->or($condition2);

        self::assertInstanceOf(OrCondition::class, $result);
    }

    public function test_reconstructs_is_distinct_from_from_ast() : void
    {
        $original = new IsDistinctFrom(
            Column::name('status'),
            Literal::null(),
            false
        );

        $node = $original->toAst();
        $reconstructed = IsDistinctFrom::fromAst($node);

        self::assertInstanceOf(IsDistinctFrom::class, $reconstructed);
        self::assertFalse($reconstructed->negated);
    }

    public function test_reconstructs_is_not_distinct_from_from_ast() : void
    {
        $original = new IsDistinctFrom(
            Column::name('status'),
            Literal::null(),
            true
        );

        $node = $original->toAst();
        $reconstructed = IsDistinctFrom::fromAst($node);

        self::assertInstanceOf(IsDistinctFrom::class, $reconstructed);
        self::assertTrue($reconstructed->negated);
    }
}
