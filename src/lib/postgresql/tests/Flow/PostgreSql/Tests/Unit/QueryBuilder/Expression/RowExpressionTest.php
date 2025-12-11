<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\{CoercionForm, Node, RowExpr};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\{AliasedExpression, RowExpression};
use PHPUnit\Framework\TestCase;

final class RowExpressionTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_args_getter() : void
    {
        $arg1 = new MockExpression('value1');
        $arg2 = new MockExpression('value2');
        $arg3 = new MockExpression('value3');

        $row = new RowExpression([$arg1, $arg2, $arg3]);

        $args = $row->args();

        self::assertCount(3, $args);
        self::assertSame($arg1, $args[0]);
        self::assertSame($arg2, $args[1]);
        self::assertSame($arg3, $args[2]);
    }

    public function test_as_returns_aliased_expression() : void
    {
        $expr = new RowExpression([new MockExpression()]);

        $aliased = $expr->as('my_row');

        self::assertInstanceOf(AliasedExpression::class, $aliased);
    }

    public function test_constructor_throws_on_empty_args() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('RowExpression requires at least 1 expression');

        new RowExpression([]);
    }

    public function test_explicit_row_format() : void
    {
        $expr = new RowExpression([new MockExpression()], true);

        $ast = $expr->toAst();

        self::assertTrue($ast->hasRowExpr());

        $rowExpr = $ast->getRowExpr();
        self::assertNotNull($rowExpr);
        self::assertSame(CoercionForm::COERCE_EXPLICIT_CALL, $rowExpr->getRowFormat());
    }

    public function test_from_ast_throws_on_empty_args() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('must have at least 1 argument');

        $rowExpr = new RowExpr();
        $rowExpr->setArgs([]);

        $node = new Node();
        $node->setRowExpr($rowExpr);

        RowExpression::fromAst($node);
    }

    public function test_from_ast_throws_on_non_row_expr() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected RowExpr node, got unknown');

        $node = new Node();
        RowExpression::fromAst($node);
    }

    public function test_implicit_row_format() : void
    {
        $expr = new RowExpression([new MockExpression()], false);

        $ast = $expr->toAst();

        self::assertTrue($ast->hasRowExpr());

        $rowExpr = $ast->getRowExpr();
        self::assertNotNull($rowExpr);
        self::assertSame(CoercionForm::COERCE_IMPLICIT_CAST, $rowExpr->getRowFormat());
    }

    public function test_is_explicit_row_getter() : void
    {
        $explicitRow = new RowExpression([new MockExpression()], true);
        $implicitRow = new RowExpression([new MockExpression()], false);

        self::assertTrue($explicitRow->isExplicitRow());
        self::assertFalse($implicitRow->isExplicitRow());
    }

    public function test_to_ast_creates_row_expr() : void
    {
        $expr = new RowExpression([
            new MockExpression('first'),
            new MockExpression('second'),
            new MockExpression('third'),
        ]);

        $ast = $expr->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasRowExpr());

        $rowExpr = $ast->getRowExpr();
        self::assertNotNull($rowExpr);
        $args = $rowExpr->getArgs();

        self::assertCount(3, $args);
    }

    public function test_with_single_argument() : void
    {
        $expr = new RowExpression([new MockExpression('single')]);

        $ast = $expr->toAst();

        self::assertTrue($ast->hasRowExpr());

        $rowExpr = $ast->getRowExpr();
        self::assertNotNull($rowExpr);
        self::assertCount(1, $rowExpr->getArgs());
    }
}
