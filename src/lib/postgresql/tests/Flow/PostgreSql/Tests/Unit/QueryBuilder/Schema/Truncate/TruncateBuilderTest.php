<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Truncate;

use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\TruncateStmt;
use Flow\PostgreSql\QueryBuilder\Schema\Truncate\TruncateBuilder;
use PHPUnit\Framework\TestCase;

use function extension_loaded;

final class TruncateBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_immutability(): void
    {
        $original = TruncateBuilder::create('users');
        $modified = $original->cascade();

        static::assertSame(DropBehavior::DROP_BEHAVIOR_UNDEFINED, $original->toAst()->getBehavior());
        static::assertSame(DropBehavior::DROP_CASCADE, $modified->toAst()->getBehavior());
    }

    public function test_simple_truncate(): void
    {
        $builder = TruncateBuilder::create('users');

        $ast = $builder->toAst();

        static::assertInstanceOf(TruncateStmt::class, $ast);
        static::assertCount(1, $ast->getRelations());
        $rangeVar = $ast->getRelations()[0]->getRangeVar();
        static::assertNotNull($rangeVar);
        static::assertSame('users', $rangeVar->getRelname());
    }

    public function test_truncate_cascade(): void
    {
        $builder = TruncateBuilder::create('users')->cascade();

        $ast = $builder->toAst();

        static::assertInstanceOf(TruncateStmt::class, $ast);
        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_truncate_continue_identity(): void
    {
        $builder = TruncateBuilder::create('users')->restartIdentity()->continueIdentity();

        $ast = $builder->toAst();

        static::assertInstanceOf(TruncateStmt::class, $ast);
        static::assertFalse($ast->getRestartSeqs());
    }

    public function test_truncate_multiple_tables(): void
    {
        $builder = TruncateBuilder::create('users', 'orders', 'products');

        $ast = $builder->toAst();

        static::assertInstanceOf(TruncateStmt::class, $ast);
        static::assertCount(3, $ast->getRelations());
        $rangeVar0 = $ast->getRelations()[0]->getRangeVar();
        $rangeVar1 = $ast->getRelations()[1]->getRangeVar();
        $rangeVar2 = $ast->getRelations()[2]->getRangeVar();
        static::assertNotNull($rangeVar0);
        static::assertNotNull($rangeVar1);
        static::assertNotNull($rangeVar2);
        static::assertSame('users', $rangeVar0->getRelname());
        static::assertSame('orders', $rangeVar1->getRelname());
        static::assertSame('products', $rangeVar2->getRelname());
    }

    public function test_truncate_restart_identity(): void
    {
        $builder = TruncateBuilder::create('users')->restartIdentity();

        $ast = $builder->toAst();

        static::assertInstanceOf(TruncateStmt::class, $ast);
        static::assertTrue($ast->getRestartSeqs());
    }

    public function test_truncate_restart_identity_cascade(): void
    {
        $builder = TruncateBuilder::create('users')->restartIdentity()->cascade();

        $ast = $builder->toAst();

        static::assertInstanceOf(TruncateStmt::class, $ast);
        static::assertTrue($ast->getRestartSeqs());
        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_truncate_restrict(): void
    {
        $builder = TruncateBuilder::create('users')->restrict();

        $ast = $builder->toAst();

        static::assertInstanceOf(TruncateStmt::class, $ast);
        static::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_truncate_with_schema(): void
    {
        $builder = TruncateBuilder::create('public.users');

        $ast = $builder->toAst();

        static::assertInstanceOf(TruncateStmt::class, $ast);
        $rangeVar = $ast->getRelations()[0]->getRangeVar();
        static::assertNotNull($rangeVar);
        static::assertSame('public', $rangeVar->getSchemaname());
        static::assertSame('users', $rangeVar->getRelname());
    }
}
