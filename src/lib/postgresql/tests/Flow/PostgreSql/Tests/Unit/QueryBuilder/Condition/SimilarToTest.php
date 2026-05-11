<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\QueryBuilder\Condition\AndCondition;
use Flow\PostgreSql\QueryBuilder\Condition\NotCondition;
use Flow\PostgreSql\QueryBuilder\Condition\OrCondition;
use Flow\PostgreSql\QueryBuilder\Condition\SimilarTo;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use PHPUnit\Framework\TestCase;

final class SimilarToTest extends TestCase
{
    public function test_and_method_returns_and_condition(): void
    {
        $condition1 = new SimilarTo(Column::name('code'), Literal::string('[0-9]{3}'));
        $condition2 = new SimilarTo(Column::name('phone'), Literal::string('[0-9]{10}'));

        $result = $condition1->and($condition2);

        static::assertInstanceOf(AndCondition::class, $result);
    }

    public function test_converts_to_ast(): void
    {
        $condition = new SimilarTo(Column::name('code'), Literal::string('[0-9]{3}-[0-9]{3}'));

        $node = $condition->toAst();

        static::assertTrue($node->hasAExpr());

        $aExpr = $node->getAExpr();
        static::assertNotNull($aExpr);
        static::assertSame(A_Expr_Kind::AEXPR_SIMILAR, $aExpr->getKind());
        static::assertTrue($aExpr->hasLexpr());
        static::assertTrue($aExpr->hasRexpr());
    }

    public function test_creates_similar_to_condition(): void
    {
        $condition = new SimilarTo(Column::name('code'), Literal::string('[0-9]{3}-[0-9]{3}'));

        static::assertInstanceOf(SimilarTo::class, $condition);
    }

    public function test_not_method_returns_not_condition(): void
    {
        $condition = new SimilarTo(Column::name('code'), Literal::string('[0-9]{3}'));

        $result = $condition->not();

        static::assertInstanceOf(NotCondition::class, $result);
    }

    public function test_or_method_returns_or_condition(): void
    {
        $condition1 = new SimilarTo(Column::name('code'), Literal::string('[0-9]{3}'));
        $condition2 = new SimilarTo(Column::name('phone'), Literal::string('[0-9]{10}'));

        $result = $condition1->or($condition2);

        static::assertInstanceOf(OrCondition::class, $result);
    }

    public function test_reconstructs_from_ast(): void
    {
        $original = new SimilarTo(Column::name('code'), Literal::string('[0-9]{3}-[0-9]{3}'));

        $node = $original->toAst();
        $reconstructed = SimilarTo::fromAst($node);

        static::assertInstanceOf(SimilarTo::class, $reconstructed);
    }
}
