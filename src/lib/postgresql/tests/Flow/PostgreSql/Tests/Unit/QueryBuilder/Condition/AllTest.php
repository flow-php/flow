<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\Protobuf\AST\SubLinkType;
use Flow\PostgreSql\QueryBuilder\Condition\All;
use Flow\PostgreSql\QueryBuilder\Condition\AndCondition;
use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;
use Flow\PostgreSql\QueryBuilder\Condition\NotCondition;
use Flow\PostgreSql\QueryBuilder\Condition\OrCondition;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use PHPUnit\Framework\TestCase;

final class AllTest extends TestCase
{
    public function test_and_method_returns_and_condition(): void
    {
        $condition1 = new All(Column::name('id'), ComparisonOperator::EQ, Literal::int(1));
        $condition2 = new All(Column::name('status'), ComparisonOperator::NEQ, Literal::string('inactive'));

        $result = $condition1->and($condition2);

        static::assertInstanceOf(AndCondition::class, $result);
    }

    public function test_converts_all_with_array_to_ast(): void
    {
        $condition = new All(Column::name('id'), ComparisonOperator::EQ, Literal::int(1));

        $node = $condition->toAst();

        static::assertTrue($node->hasAExpr());

        $aExpr = $node->getAExpr();
        static::assertNotNull($aExpr);
        static::assertSame(A_Expr_Kind::AEXPR_OP_ALL, $aExpr->getKind());
    }

    public function test_converts_all_with_subquery_to_ast(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);

        $condition = new All(Column::name('id'), ComparisonOperator::GT, $subquery);

        $node = $condition->toAst();

        static::assertTrue($node->hasSubLink());

        $subLink = $node->getSubLink();
        static::assertNotNull($subLink);
        static::assertSame(SubLinkType::ALL_SUBLINK, $subLink->getSubLinkType());
    }

    public function test_creates_all_condition_with_expression(): void
    {
        $condition = new All(Column::name('id'), ComparisonOperator::EQ, Literal::int(1));

        static::assertInstanceOf(All::class, $condition);
        static::assertSame(ComparisonOperator::EQ, $condition->operator);
    }

    public function test_creates_all_condition_with_subquery(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);

        $condition = new All(Column::name('id'), ComparisonOperator::GT, $subquery);

        static::assertInstanceOf(All::class, $condition);
        static::assertSame(ComparisonOperator::GT, $condition->operator);
        static::assertInstanceOf(Node::class, $condition->arrayOrSubquery);
    }

    public function test_not_method_returns_not_condition(): void
    {
        $condition = new All(Column::name('id'), ComparisonOperator::EQ, Literal::int(1));

        $result = $condition->not();

        static::assertInstanceOf(NotCondition::class, $result);
    }

    public function test_or_method_returns_or_condition(): void
    {
        $condition1 = new All(Column::name('id'), ComparisonOperator::EQ, Literal::int(1));
        $condition2 = new All(Column::name('status'), ComparisonOperator::NEQ, Literal::string('inactive'));

        $result = $condition1->or($condition2);

        static::assertInstanceOf(OrCondition::class, $result);
    }

    public function test_reconstructs_all_with_array_from_ast(): void
    {
        $original = new All(Column::name('id'), ComparisonOperator::LT, Literal::int(10));

        $node = $original->toAst();
        $reconstructed = All::fromAst($node);

        static::assertInstanceOf(All::class, $reconstructed);
        static::assertSame(ComparisonOperator::LT, $reconstructed->operator);
    }

    public function test_reconstructs_all_with_subquery_from_ast(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);

        $original = new All(Column::name('id'), ComparisonOperator::GT, $subquery);

        $node = $original->toAst();
        $reconstructed = All::fromAst($node);

        static::assertInstanceOf(All::class, $reconstructed);
        static::assertSame(ComparisonOperator::GT, $reconstructed->operator);
        static::assertInstanceOf(Node::class, $reconstructed->arrayOrSubquery);
    }
}
