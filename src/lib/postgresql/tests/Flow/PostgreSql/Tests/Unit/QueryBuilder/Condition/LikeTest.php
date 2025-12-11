<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\QueryBuilder\Condition\{AndCondition, Like, NotCondition, OrCondition};
use Flow\PostgreSql\QueryBuilder\Expression\{Column, Literal};
use PHPUnit\Framework\TestCase;

final class LikeTest extends TestCase
{
    public function test_and_method_returns_and_condition() : void
    {
        $condition1 = new Like(Column::name('name'), Literal::string('%test%'), false);
        $condition2 = new Like(Column::name('email'), Literal::string('%@example.com'), false);

        $result = $condition1->and($condition2);

        self::assertInstanceOf(AndCondition::class, $result);
    }

    public function test_converts_ilike_to_ast() : void
    {
        $condition = new Like(
            Column::name('name'),
            Literal::string('%test%'),
            true
        );

        $node = $condition->toAst();

        self::assertTrue($node->hasAExpr());

        $aExpr = $node->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_ILIKE, $aExpr->getKind());
    }

    public function test_converts_like_to_ast() : void
    {
        $condition = new Like(
            Column::name('name'),
            Literal::string('%test%'),
            false
        );

        $node = $condition->toAst();

        self::assertTrue($node->hasAExpr());

        $aExpr = $node->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_LIKE, $aExpr->getKind());
        self::assertTrue($aExpr->hasLexpr());
        self::assertTrue($aExpr->hasRexpr());
    }

    public function test_creates_ilike_condition() : void
    {
        $condition = new Like(
            Column::name('name'),
            Literal::string('%test%'),
            true
        );

        self::assertInstanceOf(Like::class, $condition);
        self::assertTrue($condition->caseInsensitive);
    }

    public function test_creates_like_condition() : void
    {
        $condition = new Like(
            Column::name('name'),
            Literal::string('%test%'),
            false
        );

        self::assertInstanceOf(Like::class, $condition);
        self::assertFalse($condition->caseInsensitive);
    }

    public function test_not_method_returns_not_condition() : void
    {
        $condition = new Like(Column::name('name'), Literal::string('%test%'), false);

        $result = $condition->not();

        self::assertInstanceOf(NotCondition::class, $result);
    }

    public function test_or_method_returns_or_condition() : void
    {
        $condition1 = new Like(Column::name('name'), Literal::string('%test%'), false);
        $condition2 = new Like(Column::name('email'), Literal::string('%@example.com'), false);

        $result = $condition1->or($condition2);

        self::assertInstanceOf(OrCondition::class, $result);
    }

    public function test_reconstructs_ilike_from_ast() : void
    {
        $original = new Like(
            Column::name('name'),
            Literal::string('%test%'),
            true
        );

        $node = $original->toAst();
        $reconstructed = Like::fromAst($node);

        self::assertInstanceOf(Like::class, $reconstructed);
        self::assertTrue($reconstructed->caseInsensitive);
    }

    public function test_reconstructs_like_from_ast() : void
    {
        $original = new Like(
            Column::name('name'),
            Literal::string('%test%'),
            false
        );

        $node = $original->toAst();
        $reconstructed = Like::fromAst($node);

        self::assertInstanceOf(Like::class, $reconstructed);
        self::assertFalse($reconstructed->caseInsensitive);
    }
}
