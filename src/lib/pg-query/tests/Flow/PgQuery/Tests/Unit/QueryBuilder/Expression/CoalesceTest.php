<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\{CoalesceExpr, Node, RawStmt};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use Flow\PgQuery\QueryBuilder\Expression\{AliasedExpression, Coalesce, Column, Literal};
use Flow\PgQuery\QueryBuilder\Select\SelectBuilder;
use Flow\PgQuery\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class CoalesceTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_as_returns_aliased_expression() : void
    {
        $expr = new Coalesce([new MockExpression(), new MockExpression()]);

        $aliased = $expr->as('first_non_null');

        self::assertInstanceOf(AliasedExpression::class, $aliased);
    }

    public function test_coalesce_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $expr = new Coalesce([Column::name('email'), Literal::string('no-email@example.com')]);

        $select = SelectBuilder::create()
            ->select($expr)
            ->from(new Table('users'));

        $parser = new Parser();
        $ast = $select->toAst();
        $node = new Node(['select_stmt' => $ast]);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        $deparsed = (new \Flow\PgQuery\ParsedQuery($parseResult))->deparse();

        self::assertSame("SELECT COALESCE(email, 'no-email@example.com') FROM users", $deparsed);
    }

    public function test_constructor_throws_on_less_than_two_expressions() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('COALESCE requires at least 2 expressions');

        new Coalesce([new MockExpression()]);
    }

    public function test_expressions_getter() : void
    {
        $expr1 = new MockExpression();
        $expr2 = new MockExpression();
        $expr3 = new MockExpression();

        $coalesce = new Coalesce([$expr1, $expr2, $expr3]);

        $expressions = $coalesce->expressions();

        self::assertCount(3, $expressions);
        self::assertSame($expr1, $expressions[0]);
        self::assertSame($expr2, $expressions[1]);
        self::assertSame($expr3, $expressions[2]);
    }

    public function test_from_ast_throws_on_insufficient_args() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('must have at least 2 arguments');

        $coalesceExpr = new CoalesceExpr();
        $coalesceExpr->setArgs([new Node()]);

        $node = new Node();
        $node->setCoalesceExpr($coalesceExpr);

        Coalesce::fromAst($node);
    }

    public function test_from_ast_throws_on_non_coalesce_expr() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected CoalesceExpr node, got unknown');

        $node = new Node();
        Coalesce::fromAst($node);
    }

    public function test_to_ast_creates_coalesce_expr() : void
    {
        $expr = new Coalesce([
            new MockExpression('first'),
            new MockExpression('second'),
            new MockExpression('third'),
        ]);

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasCoalesceExpr());

        $coalesceExpr = $ast->getCoalesceExpr();
        self::assertNotNull($coalesceExpr);
        $args = $coalesceExpr->getArgs();

        self::assertCount(3, $args);
    }

    public function test_with_minimum_expressions() : void
    {
        $expr = new Coalesce([new MockExpression(), new MockExpression()]);

        $ast = $expr->toAst();

        self::assertTrue($ast->hasCoalesceExpr());

        $coalesceExpr = $ast->getCoalesceExpr();
        self::assertNotNull($coalesceExpr);
        self::assertCount(2, $coalesceExpr->getArgs());
    }
}
