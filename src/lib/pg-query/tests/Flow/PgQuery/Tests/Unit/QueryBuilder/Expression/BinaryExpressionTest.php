<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Expression;

use Flow\PgQuery\Parser;
use Flow\PgQuery\Protobuf\AST\{A_Expr, A_Expr_Kind, Node, PBString, RawStmt};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use Flow\PgQuery\QueryBuilder\Expression\{AliasedExpression, BinaryExpression, Column, Literal};
use Flow\PgQuery\QueryBuilder\Select\SelectBuilder;
use Flow\PgQuery\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class BinaryExpressionTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_as_returns_aliased_expression() : void
    {
        $expr = new BinaryExpression(
            new MockExpression(),
            '+',
            new MockExpression()
        );

        $aliased = $expr->as('sum');

        self::assertInstanceOf(AliasedExpression::class, $aliased);
    }

    public function test_binary_expression_deparsed_output() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $expr = new BinaryExpression(
            Column::name('price'),
            '*',
            Literal::float(1.1)
        );

        $select = SelectBuilder::create()
            ->select($expr)
            ->from(new Table('products'));

        $parser = new Parser();
        $ast = $select->toAst();
        $node = new Node(['select_stmt' => $ast]);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        $deparsed = (new \Flow\PgQuery\ParsedQuery($parseResult))->deparse();

        self::assertSame('SELECT price * 1.1 FROM products', $deparsed);
    }

    public function test_from_ast_throws_on_non_a_expr() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected A_Expr node, got unknown');

        $node = new Node();
        BinaryExpression::fromAst($node);
    }

    public function test_from_ast_throws_on_wrong_kind() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('must be AEXPR_OP for binary expression');

        $aExpr = new A_Expr();
        $aExpr->setKind(A_Expr_Kind::AEXPR_DISTINCT);

        $node = new Node();
        $node->setAExpr($aExpr);

        BinaryExpression::fromAst($node);
    }

    public function test_from_ast_throws_when_lexpr_missing() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Missing required field "lexpr"');

        $operatorNode = new Node();
        $operatorString = new PBString();
        $operatorString->setSval('+');
        $operatorNode->setString($operatorString);

        $aExpr = new A_Expr();
        $aExpr->setKind(A_Expr_Kind::AEXPR_OP);
        $aExpr->setName([$operatorNode]);
        $aExpr->setRexpr(new Node());

        $node = new Node();
        $node->setAExpr($aExpr);

        BinaryExpression::fromAst($node);
    }

    public function test_from_ast_throws_when_name_missing() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Missing required field "name"');

        $aExpr = new A_Expr();
        $aExpr->setKind(A_Expr_Kind::AEXPR_OP);
        $aExpr->setLexpr(new Node());
        $aExpr->setRexpr(new Node());

        $node = new Node();
        $node->setAExpr($aExpr);

        BinaryExpression::fromAst($node);
    }

    public function test_from_ast_throws_when_rexpr_missing() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Missing required field "rexpr"');

        $operatorNode = new Node();
        $operatorString = new PBString();
        $operatorString->setSval('+');
        $operatorNode->setString($operatorString);

        $aExpr = new A_Expr();
        $aExpr->setKind(A_Expr_Kind::AEXPR_OP);
        $aExpr->setName([$operatorNode]);
        $aExpr->setLexpr(new Node());

        $node = new Node();
        $node->setAExpr($aExpr);

        BinaryExpression::fromAst($node);
    }

    public function test_getters() : void
    {
        $left = new MockExpression();
        $right = new MockExpression();

        $expr = new BinaryExpression($left, '+', $right);

        self::assertSame($left, $expr->left());
        self::assertSame('+', $expr->operator());
        self::assertSame($right, $expr->right());
    }

    public function test_to_ast_creates_a_expr() : void
    {
        $expr = new BinaryExpression(
            new MockExpression(),
            '+',
            new MockExpression()
        );

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasAExpr());

        $aExpr = $ast->getAExpr();
        self::assertNotNull($aExpr);
        self::assertSame(A_Expr_Kind::AEXPR_OP, $aExpr->getKind());

        $name = $aExpr->getName();
        self::assertNotNull($name);
        self::assertCount(1, $name);

        $operatorNode = $name[0];
        self::assertTrue($operatorNode->hasString());

        $operatorString = $operatorNode->getString();
        self::assertNotNull($operatorString);
        self::assertSame('+', $operatorString->getSval());

        self::assertTrue($aExpr->hasLexpr());
        self::assertTrue($aExpr->hasRexpr());
    }
}
