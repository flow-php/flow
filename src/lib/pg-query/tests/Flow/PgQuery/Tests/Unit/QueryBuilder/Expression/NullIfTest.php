<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\Protobuf\AST\{Node, NullIfExpr};
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

    public function test_from_ast_throws_on_non_null_if_expr() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected NullIfExpr node, got unknown');

        $node = new Node();
        NullIf::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_args_count() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('must have exactly 2 arguments');

        $nullIfExpr = new NullIfExpr();
        $nullIfExpr->setArgs([new Node()]);

        $node = new Node();
        $node->setNullIfExpr($nullIfExpr);

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

    public function test_to_ast_creates_null_if_expr() : void
    {
        $expr = new NullIf(
            new MockExpression('value1'),
            new MockExpression('value2')
        );

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasNullIfExpr());

        $nullIfExpr = $ast->getNullIfExpr();
        self::assertNotNull($nullIfExpr);
        $args = $nullIfExpr->getArgs();

        self::assertCount(2, $args);
    }
}
