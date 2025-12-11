<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Truncate;

use Flow\PostgreSql\Protobuf\AST\{DropBehavior, TruncateStmt};
use Flow\PostgreSql\QueryBuilder\Schema\Truncate\TruncateBuilder;
use PHPUnit\Framework\TestCase;

final class TruncateBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_immutability() : void
    {
        $original = TruncateBuilder::create('users');
        $modified = $original->cascade();

        self::assertSame(DropBehavior::DROP_BEHAVIOR_UNDEFINED, $original->toAst()->getBehavior());
        self::assertSame(DropBehavior::DROP_CASCADE, $modified->toAst()->getBehavior());
    }

    public function test_simple_truncate() : void
    {
        $builder = TruncateBuilder::create('users');

        $ast = $builder->toAst();

        self::assertInstanceOf(TruncateStmt::class, $ast);
        self::assertCount(1, $ast->getRelations());
        self::assertSame('users', $ast->getRelations()[0]->getRangeVar()->getRelname());
    }

    public function test_truncate_cascade() : void
    {
        $builder = TruncateBuilder::create('users')
            ->cascade();

        $ast = $builder->toAst();

        self::assertInstanceOf(TruncateStmt::class, $ast);
        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_truncate_continue_identity() : void
    {
        $builder = TruncateBuilder::create('users')
            ->restartIdentity()
            ->continueIdentity();

        $ast = $builder->toAst();

        self::assertInstanceOf(TruncateStmt::class, $ast);
        self::assertFalse($ast->getRestartSeqs());
    }

    public function test_truncate_multiple_tables() : void
    {
        $builder = TruncateBuilder::create('users', 'orders', 'products');

        $ast = $builder->toAst();

        self::assertInstanceOf(TruncateStmt::class, $ast);
        self::assertCount(3, $ast->getRelations());
        self::assertSame('users', $ast->getRelations()[0]->getRangeVar()->getRelname());
        self::assertSame('orders', $ast->getRelations()[1]->getRangeVar()->getRelname());
        self::assertSame('products', $ast->getRelations()[2]->getRangeVar()->getRelname());
    }

    public function test_truncate_restart_identity() : void
    {
        $builder = TruncateBuilder::create('users')
            ->restartIdentity();

        $ast = $builder->toAst();

        self::assertInstanceOf(TruncateStmt::class, $ast);
        self::assertTrue($ast->getRestartSeqs());
    }

    public function test_truncate_restart_identity_cascade() : void
    {
        $builder = TruncateBuilder::create('users')
            ->restartIdentity()
            ->cascade();

        $ast = $builder->toAst();

        self::assertInstanceOf(TruncateStmt::class, $ast);
        self::assertTrue($ast->getRestartSeqs());
        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_truncate_restrict() : void
    {
        $builder = TruncateBuilder::create('users')
            ->restrict();

        $ast = $builder->toAst();

        self::assertInstanceOf(TruncateStmt::class, $ast);
        self::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_truncate_with_schema() : void
    {
        $builder = TruncateBuilder::create('public.users');

        $ast = $builder->toAst();

        self::assertInstanceOf(TruncateStmt::class, $ast);
        self::assertSame('public', $ast->getRelations()[0]->getRangeVar()->getSchemaname());
        self::assertSame('users', $ast->getRelations()[0]->getRangeVar()->getRelname());
    }
}
