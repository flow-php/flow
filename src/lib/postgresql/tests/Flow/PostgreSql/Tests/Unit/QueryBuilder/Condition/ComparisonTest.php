<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\A_Expr;
use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Condition\AndCondition;
use Flow\PostgreSql\QueryBuilder\Condition\Comparison;
use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;
use Flow\PostgreSql\QueryBuilder\Condition\NotCondition;
use Flow\PostgreSql\QueryBuilder\Condition\OrCondition;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use PHPUnit\Framework\TestCase;

final class ComparisonTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_and_returns_and_condition(): void
    {
        $cond1 = new Comparison(Column::name('x'), ComparisonOperator::EQ, Literal::int(1));
        $cond2 = new Comparison(Column::name('y'), ComparisonOperator::GT, Literal::int(2));

        $and = $cond1->and($cond2);

        static::assertInstanceOf(AndCondition::class, $and);
    }

    public function test_comparison_eq_to_ast(): void
    {
        $comparison = new Comparison(Column::name('status'), ComparisonOperator::EQ, Literal::string('active'));

        $ast = $comparison->toAst();

        static::assertInstanceOf(Node::class, $ast);
        static::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        static::assertNotNull($aExpr);
        static::assertSame(A_Expr_Kind::AEXPR_OP, $aExpr->getKind());

        $name = $aExpr->getName();
        static::assertNotNull($name);
        static::assertCount(1, $name);

        $operatorNode = $name->offsetGet(0);
        $operatorString = $operatorNode->getString();
        static::assertNotNull($operatorString);
        static::assertSame('=', $operatorString->getSval());
    }

    public function test_comparison_gt_to_ast(): void
    {
        $comparison = new Comparison(Column::name('age'), ComparisonOperator::GT, Literal::int(18));

        $ast = $comparison->toAst();

        static::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        static::assertNotNull($aExpr);
        $name = $aExpr->getName();
        static::assertNotNull($name);
        $operatorNode = $name->offsetGet(0);
        $operatorString = $operatorNode->getString();
        static::assertNotNull($operatorString);
        static::assertSame('>', $operatorString->getSval());
    }

    public function test_comparison_gte_to_ast(): void
    {
        $comparison = new Comparison(Column::name('score'), ComparisonOperator::GTE, Literal::int(100));

        $ast = $comparison->toAst();

        static::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        static::assertNotNull($aExpr);
        $name = $aExpr->getName();
        static::assertNotNull($name);
        $operatorNode = $name->offsetGet(0);
        $operatorString = $operatorNode->getString();
        static::assertNotNull($operatorString);
        static::assertSame('>=', $operatorString->getSval());
    }

    public function test_comparison_lt_to_ast(): void
    {
        $comparison = new Comparison(Column::name('price'), ComparisonOperator::LT, Literal::float(99.99));

        $ast = $comparison->toAst();

        static::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        static::assertNotNull($aExpr);
        $name = $aExpr->getName();
        static::assertNotNull($name);
        $operatorNode = $name->offsetGet(0);
        $operatorString = $operatorNode->getString();
        static::assertNotNull($operatorString);
        static::assertSame('<', $operatorString->getSval());
    }

    public function test_comparison_lte_to_ast(): void
    {
        $comparison = new Comparison(Column::name('quantity'), ComparisonOperator::LTE, Literal::int(10));

        $ast = $comparison->toAst();

        static::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        static::assertNotNull($aExpr);
        $name = $aExpr->getName();
        static::assertNotNull($name);
        $operatorNode = $name->offsetGet(0);
        $operatorString = $operatorNode->getString();
        static::assertNotNull($operatorString);
        static::assertSame('<=', $operatorString->getSval());
    }

    public function test_comparison_neq_to_ast(): void
    {
        $comparison = new Comparison(Column::name('status'), ComparisonOperator::NEQ, Literal::string('deleted'));

        $ast = $comparison->toAst();

        static::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        static::assertNotNull($aExpr);
        $name = $aExpr->getName();
        static::assertNotNull($name);
        $operatorNode = $name->offsetGet(0);
        $operatorString = $operatorNode->getString();
        static::assertNotNull($operatorString);
        static::assertSame('<>', $operatorString->getSval());
    }

    public function test_from_ast_reconstructs_comparison(): void
    {
        $original = new Comparison(Column::name('id'), ComparisonOperator::EQ, Literal::int(42));

        $ast = $original->toAst();
        $reconstructed = Comparison::fromAst($ast);

        static::assertInstanceOf(Comparison::class, $reconstructed);

        $reconstructedAst = $reconstructed->toAst();
        static::assertTrue($reconstructedAst->hasAExpr());

        $aExpr = $reconstructedAst->getAExpr();
        static::assertNotNull($aExpr);
        static::assertSame(A_Expr_Kind::AEXPR_OP, $aExpr->getKind());
    }

    public function test_from_ast_throws_on_non_a_expr(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected A_Expr node, got unknown');

        $node = new Node();
        Comparison::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_kind(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected AEXPR_OP for Comparison');

        $aExpr = new A_Expr();
        $aExpr->setKind(A_Expr_Kind::AEXPR_IN);

        $node = new Node();
        $node->setAExpr($aExpr);

        Comparison::fromAst($node);
    }

    public function test_not_returns_not_condition(): void
    {
        $comparison = new Comparison(Column::name('active'), ComparisonOperator::EQ, Literal::bool(true));

        $not = $comparison->not();

        static::assertInstanceOf(NotCondition::class, $not);
    }

    public function test_or_returns_or_condition(): void
    {
        $cond1 = new Comparison(Column::name('x'), ComparisonOperator::EQ, Literal::int(1));
        $cond2 = new Comparison(Column::name('y'), ComparisonOperator::EQ, Literal::int(2));

        $or = $cond1->or($cond2);

        static::assertInstanceOf(OrCondition::class, $or);
    }
}
