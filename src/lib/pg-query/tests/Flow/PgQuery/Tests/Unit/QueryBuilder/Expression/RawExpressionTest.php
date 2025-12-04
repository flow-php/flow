<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\Exception\ParserException;
use Flow\PgQuery\Protobuf\AST\Node;
use Flow\PgQuery\QueryBuilder\Exception\UnsupportedNodeException;
use Flow\PgQuery\QueryBuilder\Expression\{AliasedExpression, RawExpression};
use PHPUnit\Framework\TestCase;

final class RawExpressionTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_as_returns_aliased_expression() : void
    {
        $expr = new RawExpression('1 + 1');

        $aliased = $expr->as('result');

        self::assertInstanceOf(AliasedExpression::class, $aliased);
    }

    public function test_constructor_throws_on_empty_sql() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('RawExpression SQL cannot be empty');

        new RawExpression('');
    }

    public function test_from_ast_throws_unsupported_exception() : void
    {
        $this->expectException(UnsupportedNodeException::class);
        $this->expectExceptionMessage('Cannot reconstruct RawExpression - cannot convert AST back to raw SQL string from AST');

        $node = new Node();
        RawExpression::fromAst($node);
    }

    public function test_sql_getter() : void
    {
        $sql = '1 + 1';
        $expr = new RawExpression($sql);

        self::assertSame($sql, $expr->sql());
    }

    public function test_to_ast_with_arithmetic_expression() : void
    {
        $expr = new RawExpression('1 + 2');

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasAExpr());
    }

    public function test_to_ast_with_column_reference() : void
    {
        $expr = new RawExpression('users.name');

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasColumnRef());
    }

    public function test_to_ast_with_complex_expression() : void
    {
        $expr = new RawExpression('CASE WHEN x > 0 THEN 1 ELSE 0 END');

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasCaseExpr());
    }

    public function test_to_ast_with_function_call() : void
    {
        $expr = new RawExpression('NOW()');

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasFuncCall());
    }

    public function test_to_ast_with_invalid_sql_throws_parser_exception() : void
    {
        $this->expectException(ParserException::class);

        $expr = new RawExpression('INVALID SQL @#$%');
        $expr->toAst();
    }

    public function test_to_ast_with_literal_value() : void
    {
        $expr = new RawExpression('42');

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasAConst());
    }

    public function test_to_ast_with_nested_subquery() : void
    {
        $expr = new RawExpression('(SELECT id FROM users WHERE active = true)');

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasSubLink());
    }

    public function test_to_ast_with_string_literal() : void
    {
        $expr = new RawExpression("'hello world'");

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasAConst());
    }
}
