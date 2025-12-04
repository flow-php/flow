<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Condition;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\Node;
use Flow\PgQuery\QueryBuilder\Condition\{AndCondition, NotCondition, OrCondition, RawCondition};
use Flow\PgQuery\QueryBuilder\Exception\UnsupportedNodeException;
use PHPUnit\Framework\TestCase;

final class RawConditionTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_and_returns_and_condition() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $and = $cond1->and($cond2);

        self::assertInstanceOf(AndCondition::class, $and);
    }

    public function test_from_ast_throws_unsupported_exception() : void
    {
        $this->expectException(UnsupportedNodeException::class);
        $this->expectExceptionMessage('Cannot reconstruct Flow\PgQuery\QueryBuilder\Condition\RawCondition from AST');

        $node = new Node();
        RawCondition::fromAst($node);
    }

    public function test_not_returns_not_condition() : void
    {
        $cond = new RawCondition('x = 1');
        $not = $cond->not();

        self::assertInstanceOf(NotCondition::class, $not);
    }

    public function test_or_returns_or_condition() : void
    {
        $cond1 = new RawCondition('x = 1');
        $cond2 = new RawCondition('y = 2');

        $or = $cond1->or($cond2);

        self::assertInstanceOf(OrCondition::class, $or);
    }

    public function test_to_ast_with_comparison_condition() : void
    {
        $raw = new RawCondition('id = 42');
        $ast = $raw->toAst();

        self::assertInstanceOf(Node::class, $ast);

        $parser = new Parser();
        $parsed = $parser->parse('SELECT 1 WHERE id = 42');
        $stmts = $parsed->raw()->getStmts();
        $stmt = $stmts[0]->getStmt();
        self::assertNotNull($stmt);
        $selectStmt = $stmt->getSelectStmt();
        self::assertNotNull($selectStmt);
        $expectedWhereClause = $selectStmt->getWhereClause();
        self::assertNotNull($expectedWhereClause);

        self::assertTrue($ast->hasAExpr());
        self::assertTrue($expectedWhereClause->hasAExpr());
    }

    public function test_to_ast_with_complex_condition() : void
    {
        $raw = new RawCondition('age > 18 AND status = \'active\'');
        $ast = $raw->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasBoolExpr());
    }

    public function test_to_ast_with_in_condition() : void
    {
        $raw = new RawCondition('category IN (\'A\', \'B\', \'C\')');
        $ast = $raw->toAst();

        self::assertInstanceOf(Node::class, $ast);
    }

    public function test_to_ast_with_like_condition() : void
    {
        $raw = new RawCondition('name LIKE \'%john%\'');
        $ast = $raw->toAst();

        self::assertInstanceOf(Node::class, $ast);
    }

    public function test_to_ast_with_nested_conditions() : void
    {
        $raw = new RawCondition('(x = 1 OR y = 2) AND z = 3');
        $ast = $raw->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasBoolExpr());
    }

    public function test_to_ast_with_not_condition() : void
    {
        $raw = new RawCondition('NOT active');
        $ast = $raw->toAst();

        self::assertInstanceOf(Node::class, $ast);
    }

    public function test_to_ast_with_null_check() : void
    {
        $raw = new RawCondition('email IS NOT NULL');
        $ast = $raw->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasNullTest());
    }

    public function test_to_ast_with_simple_boolean() : void
    {
        $raw = new RawCondition('active');
        $ast = $raw->toAst();

        self::assertInstanceOf(Node::class, $ast);
    }
}
