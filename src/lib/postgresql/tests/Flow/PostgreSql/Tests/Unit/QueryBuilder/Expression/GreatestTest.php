<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{MinMaxExpr, MinMaxOp, Node};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\{AliasedExpression, Greatest};
use PHPUnit\Framework\TestCase;

final class GreatestTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_as_returns_aliased_expression() : void
    {
        $expr = new Greatest([new MockExpression(), new MockExpression()]);

        $aliased = $expr->as('max_value');

        self::assertInstanceOf(AliasedExpression::class, $aliased);
    }

    public function test_constructor_throws_on_less_than_two_expressions() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('GREATEST requires at least 2 expressions');

        new Greatest([new MockExpression()]);
    }

    public function test_expressions_getter() : void
    {
        $expr1 = new MockExpression();
        $expr2 = new MockExpression();
        $expr3 = new MockExpression();

        $greatest = new Greatest([$expr1, $expr2, $expr3]);

        $expressions = $greatest->expressions();

        self::assertCount(3, $expressions);
        self::assertSame($expr1, $expressions[0]);
        self::assertSame($expr2, $expressions[1]);
        self::assertSame($expr3, $expressions[2]);
    }

    public function test_from_ast_throws_on_insufficient_args() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('must have at least 2 arguments');

        $minMaxExpr = new MinMaxExpr();
        $minMaxExpr->setOp(MinMaxOp::IS_GREATEST);
        $minMaxExpr->setArgs([new Node()]);

        $node = new Node();
        $node->setMinMaxExpr($minMaxExpr);

        Greatest::fromAst($node);
    }

    public function test_from_ast_throws_on_non_min_max_expr() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected MinMaxExpr node, got unknown');

        $node = new Node();
        Greatest::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_op() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('must be IS_GREATEST');

        $minMaxExpr = new MinMaxExpr();
        $minMaxExpr->setOp(MinMaxOp::IS_LEAST);
        $minMaxExpr->setArgs([new Node(), new Node()]);

        $node = new Node();
        $node->setMinMaxExpr($minMaxExpr);

        Greatest::fromAst($node);
    }

    public function test_to_ast_creates_min_max_expr() : void
    {
        $expr = new Greatest([
            new MockExpression('first'),
            new MockExpression('second'),
            new MockExpression('third'),
        ]);

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasMinMaxExpr());

        $minMaxExpr = $ast->getMinMaxExpr();
        self::assertNotNull($minMaxExpr);
        self::assertSame(MinMaxOp::IS_GREATEST, $minMaxExpr->getOp());

        $args = $minMaxExpr->getArgs();
        self::assertCount(3, $args);
    }

    public function test_with_minimum_expressions() : void
    {
        $expr = new Greatest([new MockExpression(), new MockExpression()]);

        $ast = $expr->toAst();

        self::assertTrue($ast->hasMinMaxExpr());

        $minMaxExpr = $ast->getMinMaxExpr();
        self::assertNotNull($minMaxExpr);
        self::assertCount(2, $minMaxExpr->getArgs());
    }
}
