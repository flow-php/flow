<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\MinMaxExpr;
use Flow\PostgreSql\Protobuf\AST\MinMaxOp;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\Least;
use InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function extension_loaded;

final class LeastTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_as_returns_aliased_expression(): void
    {
        $expr = new Least([new MockExpression(), new MockExpression()]);

        $aliased = $expr->as('min_value');

        static::assertInstanceOf(AliasedExpression::class, $aliased);
    }

    public function test_constructor_throws_on_less_than_two_expressions(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('LEAST requires at least 2 expressions');

        new Least([new MockExpression()]);
    }

    public function test_expressions_getter(): void
    {
        $expr1 = new MockExpression();
        $expr2 = new MockExpression();
        $expr3 = new MockExpression();

        $least = new Least([$expr1, $expr2, $expr3]);

        $expressions = $least->expressions();

        static::assertCount(3, $expressions);
        static::assertSame($expr1, $expressions[0]);
        static::assertSame($expr2, $expressions[1]);
        static::assertSame($expr3, $expressions[2]);
    }

    public function test_from_ast_throws_on_insufficient_args(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('must have at least 2 arguments');

        $minMaxExpr = new MinMaxExpr();
        $minMaxExpr->setOp(MinMaxOp::IS_LEAST);
        $minMaxExpr->setArgs([new Node()]);

        $node = new Node();
        $node->setMinMaxExpr($minMaxExpr);

        Least::fromAst($node);
    }

    public function test_from_ast_throws_on_non_min_max_expr(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected MinMaxExpr node, got unknown');

        $node = new Node();
        Least::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_op(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('must be IS_LEAST');

        $minMaxExpr = new MinMaxExpr();
        $minMaxExpr->setOp(MinMaxOp::IS_GREATEST);
        $minMaxExpr->setArgs([new Node(), new Node()]);

        $node = new Node();
        $node->setMinMaxExpr($minMaxExpr);

        Least::fromAst($node);
    }

    public function test_to_ast_creates_min_max_expr(): void
    {
        $expr = new Least([
            new MockExpression('first'),
            new MockExpression('second'),
            new MockExpression('third'),
        ]);

        $ast = $expr->toAst();

        static::assertInstanceOf(Node::class, $ast);
        static::assertTrue($ast->hasMinMaxExpr());

        $minMaxExpr = $ast->getMinMaxExpr();
        static::assertNotNull($minMaxExpr);
        static::assertSame(MinMaxOp::IS_LEAST, $minMaxExpr->getOp());

        $args = $minMaxExpr->getArgs();
        static::assertCount(3, $args);
    }

    public function test_with_minimum_expressions(): void
    {
        $expr = new Least([new MockExpression(), new MockExpression()]);

        $ast = $expr->toAst();

        static::assertTrue($ast->hasMinMaxExpr());

        $minMaxExpr = $ast->getMinMaxExpr();
        static::assertNotNull($minMaxExpr);
        static::assertCount(2, $minMaxExpr->getArgs());
    }
}
