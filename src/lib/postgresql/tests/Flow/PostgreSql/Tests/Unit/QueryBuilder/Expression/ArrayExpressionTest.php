<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Expression;

use Flow\PostgreSql\Protobuf\AST\A_ArrayExpr;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\AliasedExpression;
use Flow\PostgreSql\QueryBuilder\Expression\ArrayExpression;
use PHPUnit\Framework\TestCase;

final class ArrayExpressionTest extends TestCase
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
        $expr = new ArrayExpression([new MockExpression()]);

        $aliased = $expr->as('my_array');

        static::assertInstanceOf(AliasedExpression::class, $aliased);
    }

    public function test_elements_getter(): void
    {
        $elem1 = new MockExpression('value1');
        $elem2 = new MockExpression('value2');
        $elem3 = new MockExpression('value3');

        $array = new ArrayExpression([$elem1, $elem2, $elem3]);

        $elements = $array->elements();

        static::assertCount(3, $elements);
        static::assertSame($elem1, $elements[0]);
        static::assertSame($elem2, $elements[1]);
        static::assertSame($elem3, $elements[2]);
    }

    public function test_empty_array_expression(): void
    {
        $expr = new ArrayExpression([]);

        $ast = $expr->toAst();

        static::assertInstanceOf(Node::class, $ast);
        static::assertTrue($ast->hasAArrayExpr());

        $arrayExpr = $ast->getAArrayExpr();
        static::assertNotNull($arrayExpr);
        static::assertCount(0, $arrayExpr->getElements());
    }

    public function test_from_ast_throws_on_non_array_expr(): void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected A_ArrayExpr node, got unknown');

        $node = new Node();
        ArrayExpression::fromAst($node);
    }

    public function test_from_ast_with_empty_elements(): void
    {
        $arrayExpr = new A_ArrayExpr();
        $arrayExpr->setElements([]);

        $node = new Node();
        $node->setAArrayExpr($arrayExpr);

        $result = ArrayExpression::fromAst($node);

        static::assertCount(0, $result->elements());
    }

    public function test_to_ast_creates_array_expr(): void
    {
        $expr = new ArrayExpression([
            new MockExpression('first'),
            new MockExpression('second'),
            new MockExpression('third'),
        ]);

        $ast = $expr->toAst();

        static::assertInstanceOf(Node::class, $ast);
        static::assertTrue($ast->hasAArrayExpr());

        $arrayExpr = $ast->getAArrayExpr();
        static::assertNotNull($arrayExpr);
        $elements = $arrayExpr->getElements();

        static::assertCount(3, $elements);
    }

    public function test_with_single_element(): void
    {
        $expr = new ArrayExpression([new MockExpression('single')]);

        $ast = $expr->toAst();

        static::assertTrue($ast->hasAArrayExpr());

        $arrayExpr = $ast->getAArrayExpr();
        static::assertNotNull($arrayExpr);
        static::assertCount(1, $arrayExpr->getElements());
    }
}
