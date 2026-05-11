<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\CoercionForm;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RowExpr;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\RowExpression;
use PHPUnit\Framework\TestCase;

final class RowExpressionTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_args_getter(): void
    {
        $arg1 = new MockExpression('value1');
        $arg2 = new MockExpression('value2');
        $arg3 = new MockExpression('value3');

        $row = new RowExpression([$arg1, $arg2, $arg3]);

        $args = $row->args();

        static::assertCount(3, $args);
        static::assertSame($arg1, $args[0]);
        static::assertSame($arg2, $args[1]);
        static::assertSame($arg3, $args[2]);
    }

    public function test_as_returns_aliased_expression(): void
    {
        $expr = new RowExpression([new MockExpression()]);

        $aliased = $expr->as('my_row');

        static::assertInstanceOf(AliasedExpression::class, $aliased);
    }

    public function test_constructor_throws_on_empty_args(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('RowExpression requires at least 1 expression');

        new RowExpression([]);
    }

    public function test_explicit_row_format(): void
    {
        $expr = new RowExpression([new MockExpression()], true);

        $ast = $expr->toAst();

        static::assertTrue($ast->hasRowExpr());

        $rowExpr = $ast->getRowExpr();
        static::assertNotNull($rowExpr);
        static::assertSame(CoercionForm::COERCE_EXPLICIT_CALL, $rowExpr->getRowFormat());
    }

    public function test_from_ast_throws_on_empty_args(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('must have at least 1 argument');

        $rowExpr = new RowExpr();
        $rowExpr->setArgs([]);

        $node = new Node();
        $node->setRowExpr($rowExpr);

        RowExpression::fromAst($node);
    }

    public function test_from_ast_throws_on_non_row_expr(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected RowExpr node, got unknown');

        $node = new Node();
        RowExpression::fromAst($node);
    }

    public function test_implicit_row_format(): void
    {
        $expr = new RowExpression([new MockExpression()], false);

        $ast = $expr->toAst();

        static::assertTrue($ast->hasRowExpr());

        $rowExpr = $ast->getRowExpr();
        static::assertNotNull($rowExpr);
        static::assertSame(CoercionForm::COERCE_IMPLICIT_CAST, $rowExpr->getRowFormat());
    }

    public function test_is_explicit_row_getter(): void
    {
        $explicitRow = new RowExpression([new MockExpression()], true);
        $implicitRow = new RowExpression([new MockExpression()], false);

        static::assertTrue($explicitRow->isExplicitRow());
        static::assertFalse($implicitRow->isExplicitRow());
    }

    public function test_to_ast_creates_row_expr(): void
    {
        $expr = new RowExpression([
            new MockExpression('first'),
            new MockExpression('second'),
            new MockExpression('third'),
        ]);

        $ast = $expr->toAst();

        static::assertInstanceOf(Node::class, $ast);
        static::assertTrue($ast->hasRowExpr());

        $rowExpr = $ast->getRowExpr();
        static::assertNotNull($rowExpr);
        $args = $rowExpr->getArgs();

        static::assertCount(3, $args);
    }

    public function test_with_single_argument(): void
    {
        $expr = new RowExpression([new MockExpression('single')]);

        $ast = $expr->toAst();

        static::assertTrue($ast->hasRowExpr());

        $rowExpr = $ast->getRowExpr();
        static::assertNotNull($rowExpr);
        static::assertCount(1, $rowExpr->getArgs());
    }
}
