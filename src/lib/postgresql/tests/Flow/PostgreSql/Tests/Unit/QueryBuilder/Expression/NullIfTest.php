<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\A_Expr;
use Flow\PostgreSql\Protobuf\AST\A_Expr_Kind;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\NullIf;
use PHPUnit\Framework\TestCase;

final class NullIfTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_as_returns_aliased_expression(): void
    {
        $expr = new NullIf(new MockExpression(), new MockExpression());

        $aliased = $expr->as('nullable');

        static::assertInstanceOf(AliasedExpression::class, $aliased);
    }

    public function test_from_ast_throws_on_non_a_expr(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected A_Expr node, got unknown');

        $node = new Node();
        NullIf::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_a_expr_kind(): void
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

    public function test_getters(): void
    {
        $first = new MockExpression('first');
        $second = new MockExpression('second');

        $expr = new NullIf($first, $second);

        static::assertSame($first, $expr->first());
        static::assertSame($second, $expr->second());
    }

    public function test_to_ast_creates_a_expr_with_aexpr_nullif(): void
    {
        $expr = new NullIf(new MockExpression('value1'), new MockExpression('value2'));

        $ast = $expr->toAst();

        static::assertInstanceOf(Node::class, $ast);
        static::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        static::assertNotNull($aExpr);
        static::assertSame(A_Expr_Kind::AEXPR_NULLIF, $aExpr->getKind());

        static::assertNotNull($aExpr->getLexpr());
        static::assertNotNull($aExpr->getRexpr());
    }
}
