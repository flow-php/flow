<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\QueryBuilder\Condition\AndCondition;
use Flow\PostgreSql\QueryBuilder\Condition\Like;
use Flow\PostgreSql\QueryBuilder\Condition\NotCondition;
use Flow\PostgreSql\QueryBuilder\Condition\OrCondition;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use PHPUnit\Framework\TestCase;

final class LikeTest extends TestCase
{
    public function test_and_method_returns_and_condition(): void
    {
        $condition1 = new Like(Column::name('name'), Literal::string('%test%'), false);
        $condition2 = new Like(Column::name('email'), Literal::string('%@example.com'), false);

        $result = $condition1->and($condition2);

        static::assertInstanceOf(AndCondition::class, $result);
    }

    public function test_converts_ilike_to_ast(): void
    {
        $condition = new Like(Column::name('name'), Literal::string('%test%'), true);

        $node = $condition->toAst();

        static::assertTrue($node->hasAExpr());

        $aExpr = $node->getAExpr();
        static::assertNotNull($aExpr);
        static::assertSame(A_Expr_Kind::AEXPR_ILIKE, $aExpr->getKind());
    }

    public function test_converts_like_to_ast(): void
    {
        $condition = new Like(Column::name('name'), Literal::string('%test%'), false);

        $node = $condition->toAst();

        static::assertTrue($node->hasAExpr());

        $aExpr = $node->getAExpr();
        static::assertNotNull($aExpr);
        static::assertSame(A_Expr_Kind::AEXPR_LIKE, $aExpr->getKind());
        static::assertTrue($aExpr->hasLexpr());
        static::assertTrue($aExpr->hasRexpr());
    }

    public function test_creates_ilike_condition(): void
    {
        $condition = new Like(Column::name('name'), Literal::string('%test%'), true);

        static::assertInstanceOf(Like::class, $condition);
        static::assertTrue($condition->caseInsensitive);
    }

    public function test_creates_like_condition(): void
    {
        $condition = new Like(Column::name('name'), Literal::string('%test%'), false);

        static::assertInstanceOf(Like::class, $condition);
        static::assertFalse($condition->caseInsensitive);
    }

    public function test_not_method_returns_not_condition(): void
    {
        $condition = new Like(Column::name('name'), Literal::string('%test%'), false);

        $result = $condition->not();

        static::assertInstanceOf(NotCondition::class, $result);
    }

    public function test_or_method_returns_or_condition(): void
    {
        $condition1 = new Like(Column::name('name'), Literal::string('%test%'), false);
        $condition2 = new Like(Column::name('email'), Literal::string('%@example.com'), false);

        $result = $condition1->or($condition2);

        static::assertInstanceOf(OrCondition::class, $result);
    }

    public function test_reconstructs_ilike_from_ast(): void
    {
        $original = new Like(Column::name('name'), Literal::string('%test%'), true);

        $node = $original->toAst();
        $reconstructed = Like::fromAst($node);

        static::assertInstanceOf(Like::class, $reconstructed);
        static::assertTrue($reconstructed->caseInsensitive);
    }

    public function test_reconstructs_like_from_ast(): void
    {
        $original = new Like(Column::name('name'), Literal::string('%test%'), false);

        $node = $original->toAst();
        $reconstructed = Like::fromAst($node);

        static::assertInstanceOf(Like::class, $reconstructed);
        static::assertFalse($reconstructed->caseInsensitive);
    }
}
