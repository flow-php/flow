<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\A_Expr;
use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Condition\AndCondition;
use Flow\PostgreSql\QueryBuilder\Condition\In;
use Flow\PostgreSql\QueryBuilder\Condition\NotCondition;
use Flow\PostgreSql\QueryBuilder\Condition\OrCondition;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use Flow\PostgreSql\QueryBuilder\Expression\Literal;
use PHPUnit\Framework\TestCase;

final class InTest extends TestCase
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
        $cond1 = new In(Column::name('x'), [Literal::int(1), Literal::int(2)]);
        $cond2 = new In(Column::name('y'), [Literal::int(3), Literal::int(4)]);

        $and = $cond1->and($cond2);

        static::assertInstanceOf(AndCondition::class, $and);
    }

    public function test_from_ast_reconstructs_in(): void
    {
        $original = new In(Column::name('status'), [
            Literal::string('active'),
            Literal::string('pending'),
            Literal::string('completed'),
        ]);

        $ast = $original->toAst();
        $reconstructed = In::fromAst($ast);

        static::assertInstanceOf(In::class, $reconstructed);
        static::assertCount(3, $reconstructed->values);

        $reconstructedAst = $reconstructed->toAst();
        static::assertTrue($reconstructedAst->hasAExpr());

        $aExpr = $reconstructedAst->getAExpr();
        static::assertNotNull($aExpr);
        static::assertSame(A_Expr_Kind::AEXPR_IN, $aExpr->getKind());
    }

    public function test_from_ast_throws_on_non_a_expr(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected A_Expr node, got unknown');

        $node = new Node();
        In::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_kind(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected AEXPR_IN for In condition');

        $aExpr = new A_Expr();
        $aExpr->setKind(A_Expr_Kind::AEXPR_OP);

        $node = new Node();
        $node->setAExpr($aExpr);

        In::fromAst($node);
    }

    public function test_in_empty_values_throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('IN condition requires at least 1 value');

        new In(Column::name('id'), []);
    }

    public function test_in_multiple_values_to_ast(): void
    {
        $in = new In(Column::name('category_id'), [
            Literal::int(1),
            Literal::int(2),
            Literal::int(3),
            Literal::int(5),
        ]);

        $ast = $in->toAst();

        static::assertInstanceOf(Node::class, $ast);
        static::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        static::assertNotNull($aExpr);
        static::assertSame(A_Expr_Kind::AEXPR_IN, $aExpr->getKind());

        $rexpr = $aExpr->getRexpr();
        static::assertNotNull($rexpr);
        static::assertTrue($rexpr->hasList());

        $list = $rexpr->getList();
        static::assertNotNull($list);
        $items = $list->getItems();
        static::assertCount(4, $items);
    }

    public function test_in_single_value_to_ast(): void
    {
        $in = new In(Column::name('status'), [Literal::string('active')]);

        $ast = $in->toAst();

        static::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        static::assertNotNull($aExpr);
        static::assertSame(A_Expr_Kind::AEXPR_IN, $aExpr->getKind());

        $rexpr = $aExpr->getRexpr();
        static::assertNotNull($rexpr);

        $list = $rexpr->getList();
        static::assertNotNull($list);
        $items = $list->getItems();
        static::assertCount(1, $items);
    }

    public function test_not_returns_not_condition(): void
    {
        $in = new In(Column::name('id'), [Literal::int(1), Literal::int(2)]);

        $not = $in->not();

        static::assertInstanceOf(NotCondition::class, $not);
    }

    public function test_or_returns_or_condition(): void
    {
        $cond1 = new In(Column::name('x'), [Literal::int(1), Literal::int(2)]);
        $cond2 = new In(Column::name('y'), [Literal::int(3), Literal::int(4)]);

        $or = $cond1->or($cond2);

        static::assertInstanceOf(OrCondition::class, $or);
    }
}
