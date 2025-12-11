<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Condition;

use Flow\PostgreSql\Protobuf\AST\{Node, NullTestType};
use Flow\PostgreSql\QueryBuilder\Condition\{AndCondition, IsNull, NotCondition, OrCondition};
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use PHPUnit\Framework\TestCase;

final class IsNullTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_and_returns_and_condition() : void
    {
        $cond1 = new IsNull(Column::name('x'));
        $cond2 = new IsNull(Column::name('y'));

        $and = $cond1->and($cond2);

        self::assertInstanceOf(AndCondition::class, $and);
    }

    public function test_from_ast_reconstructs_is_not_null() : void
    {
        $original = new IsNull(
            Column::name('email'),
            true
        );

        $ast = $original->toAst();
        $reconstructed = IsNull::fromAst($ast);

        self::assertInstanceOf(IsNull::class, $reconstructed);
        self::assertTrue($reconstructed->negated);

        $reconstructedAst = $reconstructed->toAst();
        self::assertTrue($reconstructedAst->hasNullTest());

        $nullTest = $reconstructedAst->getNullTest();
        self::assertNotNull($nullTest);
        self::assertSame(NullTestType::IS_NOT_NULL, $nullTest->getNulltesttype());
    }

    public function test_from_ast_reconstructs_is_null() : void
    {
        $original = new IsNull(
            Column::name('deleted_at')
        );

        $ast = $original->toAst();
        $reconstructed = IsNull::fromAst($ast);

        self::assertInstanceOf(IsNull::class, $reconstructed);
        self::assertFalse($reconstructed->negated);

        $reconstructedAst = $reconstructed->toAst();
        self::assertTrue($reconstructedAst->hasNullTest());

        $nullTest = $reconstructedAst->getNullTest();
        self::assertNotNull($nullTest);
        self::assertSame(NullTestType::IS_NULL, $nullTest->getNulltesttype());
    }

    public function test_from_ast_throws_on_non_null_test() : void
    {
        $this->expectException(InvalidAstException::class);
        $this->expectExceptionMessage('Expected NullTest node, got unknown');

        $node = new Node();
        IsNull::fromAst($node);
    }

    public function test_is_not_null_to_ast() : void
    {
        $isNull = new IsNull(
            Column::name('description'),
            true
        );

        $ast = $isNull->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasNullTest());

        $nullTest = $ast->getNullTest();
        self::assertNotNull($nullTest);
        self::assertSame(NullTestType::IS_NOT_NULL, $nullTest->getNulltesttype());
        self::assertNotNull($nullTest->getArg());
    }

    public function test_is_null_to_ast() : void
    {
        $isNull = new IsNull(
            Column::name('deleted_at')
        );

        $ast = $isNull->toAst();

        self::assertInstanceOf(Node::class, $ast);
        self::assertTrue($ast->hasNullTest());

        $nullTest = $ast->getNullTest();
        self::assertNotNull($nullTest);
        self::assertSame(NullTestType::IS_NULL, $nullTest->getNulltesttype());
        self::assertNotNull($nullTest->getArg());
    }

    public function test_not_returns_not_condition() : void
    {
        $isNull = new IsNull(Column::name('value'));

        $not = $isNull->not();

        self::assertInstanceOf(NotCondition::class, $not);
    }

    public function test_or_returns_or_condition() : void
    {
        $cond1 = new IsNull(Column::name('x'));
        $cond2 = new IsNull(Column::name('y'));

        $or = $cond1->or($cond2);

        self::assertInstanceOf(OrCondition::class, $or);
    }
}
