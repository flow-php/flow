<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\{A_Expr, A_Expr_Kind, Node};
use Flow\PostgreSql\QueryBuilder\Condition\{AndCondition, Between, NotCondition, OrCondition};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\{Column, Literal};
use PHPUnit\Framework\TestCase;

final class BetweenTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_and_returns_and_condition() : void
    {
        $cond1 = new Between(Column::name('x'), Literal::int(1), Literal::int(10));
        $cond2 = new Between(Column::name('y'), Literal::int(20), Literal::int(30));

        $and = $cond1->and($cond2);

        self::assertInstanceOf(AndCondition::class, $and);
    }

    public function test_between_basic_to_ast() : void
    {
        $between = new Between(
            Column::name('age'),
            Literal::int(18),
            Literal::int(65)
        );

        $ast = $between->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_BETWEEN, $aExpr->getKind());

        $rexpr = $aExpr->getRexpr();
        self::assertNotNull($rexpr);
        self::assertTrue($rexpr->hasList());

        $list = $rexpr->getList();
        self::assertNotNull($list);
        $items = $list->getItems();
        self::assertCount(2, $items);
    }

    public function test_between_negated_to_ast() : void
    {
        $between = new Between(
            Column::name('score'),
            Literal::int(0),
            Literal::int(50),
            false,
            true
        );

        $ast = $between->toAst();

        self::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_NOT_BETWEEN, $aExpr->getKind());
    }

    public function test_between_symmetric_negated_to_ast() : void
    {
        $between = new Between(
            Column::name('value'),
            Literal::int(1),
            Literal::int(100),
            true,
            true
        );

        $ast = $between->toAst();

        self::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM, $aExpr->getKind());
    }

    public function test_between_symmetric_to_ast() : void
    {
        $between = new Between(
            Column::name('price'),
            Literal::float(10.0),
            Literal::float(100.0),
            true,
            false
        );

        $ast = $between->toAst();

        self::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_BETWEEN_SYM, $aExpr->getKind());
    }

    public function test_from_ast_reconstructs_between() : void
    {
        $original = new Between(
            Column::name('date'),
            Literal::string('2024-01-01'),
            Literal::string('2024-12-31')
        );

        $ast = $original->toAst();
        $reconstructed = Between::fromAst($ast);

        self::assertInstanceOf(Between::class, $reconstructed);
        self::assertFalse($reconstructed->symmetric);
        self::assertFalse($reconstructed->negated);

        $reconstructedAst = $reconstructed->toAst();
        self::assertTrue($reconstructedAst->hasAExpr());

        $aExpr = $reconstructedAst->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_BETWEEN, $aExpr->getKind());
    }

    public function test_from_ast_reconstructs_negated_between() : void
    {
        $original = new Between(
            Column::name('id'),
            Literal::int(1),
            Literal::int(10),
            false,
            true
        );

        $ast = $original->toAst();
        $reconstructed = Between::fromAst($ast);

        self::assertInstanceOf(Between::class, $reconstructed);
        self::assertFalse($reconstructed->symmetric);
        self::assertTrue($reconstructed->negated);
    }

    public function test_from_ast_reconstructs_symmetric_between() : void
    {
        $original = new Between(
            Column::name('value'),
            Literal::int(10),
            Literal::int(20),
            true,
            false
        );

        $ast = $original->toAst();
        $reconstructed = Between::fromAst($ast);

        self::assertInstanceOf(Between::class, $reconstructed);
        self::assertTrue($reconstructed->symmetric);
        self::assertFalse($reconstructed->negated);
    }

    public function test_from_ast_throws_on_non_a_expr() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected A_Expr node, got unknown');

        $node = new Node();
        Between::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_kind() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected BETWEEN variant for Between condition');

        $aExpr = new A_Expr();
        $aExpr->setKind(A_Expr_Kind::AEXPR_OP);

        $node = new Node();
        $node->setAExpr($aExpr);

        Between::fromAst($node);
    }

    public function test_not_returns_not_condition() : void
    {
        $between = new Between(
            Column::name('age'),
            Literal::int(18),
            Literal::int(65)
        );

        $not = $between->not();

        self::assertInstanceOf(NotCondition::class, $not);
    }

    public function test_or_returns_or_condition() : void
    {
        $cond1 = new Between(Column::name('x'), Literal::int(1), Literal::int(10));
        $cond2 = new Between(Column::name('y'), Literal::int(20), Literal::int(30));

        $or = $cond1->or($cond2);

        self::assertInstanceOf(OrCondition::class, $or);
    }
}
