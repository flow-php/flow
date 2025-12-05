<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{A_Expr, A_Expr_Kind, Node};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use Flow\PgQuery\QueryBuilder\Expression\{AliasedExpression, NullIf};
use PHPUnit\Framework\TestCase;

final class NullIfTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_as_returns_aliased_expression() : void
    {
        $expr = new NullIf(new MockExpression(), new MockExpression());

        $aliased = $expr->as('nullable');

        self::assertInstanceOf(AliasedExpression::class, $aliased);
    }

    public function test_from_ast_throws_on_non_a_expr() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected A_Expr node, got unknown');

        $node = new Node();
        NullIf::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_a_expr_kind() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected AEXPR_NULLIF for NullIf expression');

        $aExpr = new A_Expr();
        $aExpr->setKind(A_Expr_Kind::AEXPR_OP);
        $aExpr->setLexpr(new Node());
        $aExpr->setRexpr(new Node());

        $node = new Node();
        $node->setAExpr($aExpr);

        NullIf::fromAst($node);
    }

    public function test_getters() : void
    {
        $first = new MockExpression('first');
        $second = new MockExpression('second');

        $expr = new NullIf($first, $second);

        self::assertSame($first, $expr->first());
        self::assertSame($second, $expr->second());
    }

    public function test_to_ast_creates_a_expr_with_aexpr_nullif() : void
    {
        $expr = new NullIf(
            new MockExpression('value1'),
            new MockExpression('value2')
        );

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_NULLIF, $aExpr->getKind());

        self::assertNotNull($aExpr->getLexpr());
        self::assertNotNull($aExpr->getRexpr());
    }
}
