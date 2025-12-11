<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\{A_Expr_Kind, Node, SelectStmt, SubLinkType};
use Flow\PostgreSql\QueryBuilder\Condition\{All, AndCondition, ComparisonOperator, NotCondition, OrCondition};
use Flow\PostgreSql\QueryBuilder\Expression\{Column, Literal};
use PHPUnit\Framework\TestCase;

final class AllTest extends TestCase
{
    public function test_and_method_returns_and_condition() : void
    {
        $condition1 = new All(Column::name('id'), ComparisonOperator::EQ, Literal::int(1));
        $condition2 = new All(Column::name('status'), ComparisonOperator::NEQ, Literal::string('inactive'));

        $result = $condition1->and($condition2);

        self::assertInstanceOf(AndCondition::class, $result);
    }

    public function test_converts_all_with_array_to_ast() : void
    {
        $condition = new All(
            Column::name('id'),
            ComparisonOperator::EQ,
            Literal::int(1)
        );

        $node = $condition->toAst();

        self::assertTrue($node->hasAExpr());

        $aExpr = $node->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_OP_ALL, $aExpr->getKind());
    }

    public function test_converts_all_with_subquery_to_ast() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);

        $condition = new All(
            Column::name('id'),
            ComparisonOperator::GT,
            $subquery
        );

        $node = $condition->toAst();

        self::assertTrue($node->hasSubLink());

        $subLink = $node->getSubLink();
        self::assertNotNull($subLink);
        self::assertSame(SubLinkType::ALL_SUBLINK, $subLink->getSubLinkType());
    }

    public function test_creates_all_condition_with_expression() : void
    {
        $condition = new All(
            Column::name('id'),
            ComparisonOperator::EQ,
            Literal::int(1)
        );

        self::assertInstanceOf(All::class, $condition);
        self::assertSame(ComparisonOperator::EQ, $condition->operator);
    }

    public function test_creates_all_condition_with_subquery() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);

        $condition = new All(
            Column::name('id'),
            ComparisonOperator::GT,
            $subquery
        );

        self::assertInstanceOf(All::class, $condition);
        self::assertSame(ComparisonOperator::GT, $condition->operator);
        self::assertInstanceOf(Node::class, $condition->arrayOrSubquery);
    }

    public function test_not_method_returns_not_condition() : void
    {
        $condition = new All(Column::name('id'), ComparisonOperator::EQ, Literal::int(1));

        $result = $condition->not();

        self::assertInstanceOf(NotCondition::class, $result);
    }

    public function test_or_method_returns_or_condition() : void
    {
        $condition1 = new All(Column::name('id'), ComparisonOperator::EQ, Literal::int(1));
        $condition2 = new All(Column::name('status'), ComparisonOperator::NEQ, Literal::string('inactive'));

        $result = $condition1->or($condition2);

        self::assertInstanceOf(OrCondition::class, $result);
    }

    public function test_reconstructs_all_with_array_from_ast() : void
    {
        $original = new All(
            Column::name('id'),
            ComparisonOperator::LT,
            Literal::int(10)
        );

        $node = $original->toAst();
        $reconstructed = All::fromAst($node);

        self::assertInstanceOf(All::class, $reconstructed);
        self::assertSame(ComparisonOperator::LT, $reconstructed->operator);
    }

    public function test_reconstructs_all_with_subquery_from_ast() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);

        $original = new All(
            Column::name('id'),
            ComparisonOperator::GT,
            $subquery
        );

        $node = $original->toAst();
        $reconstructed = All::fromAst($node);

        self::assertInstanceOf(All::class, $reconstructed);
        self::assertSame(ComparisonOperator::GT, $reconstructed->operator);
        self::assertInstanceOf(Node::class, $reconstructed->arrayOrSubquery);
    }
}
