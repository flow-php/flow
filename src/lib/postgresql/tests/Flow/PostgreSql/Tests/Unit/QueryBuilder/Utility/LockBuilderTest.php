<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Utility;

use Flow\PostgreSql\Protobuf\AST\LockStmt;
use Flow\PostgreSql\QueryBuilder\Utility\LockBuilder;
use Flow\PostgreSql\QueryBuilder\Utility\LockMode;
use PHPUnit\Framework\TestCase;

final class LockBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_basic_lock(): void
    {
        $builder = LockBuilder::create('users');

        $ast = $builder->toAst();

        static::assertInstanceOf(LockStmt::class, $ast);
        static::assertCount(1, $ast->getRelations());
        static::assertSame('users', $ast->getRelations()[0]->getRangeVar()?->getRelname());
    }

    public function test_immutability(): void
    {
        $original = LockBuilder::create('users');
        $modified = $original->share();

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        static::assertSame(LockMode::ACCESS_EXCLUSIVE->value, $originalAst->getMode());
        static::assertSame(LockMode::SHARE->value, $modifiedAst->getMode());
    }

    public function test_lock_access_exclusive_mode(): void
    {
        $builder = LockBuilder::create('users')->accessExclusive();

        $ast = $builder->toAst();

        static::assertSame(LockMode::ACCESS_EXCLUSIVE->value, $ast->getMode());
    }

    public function test_lock_access_share_mode(): void
    {
        $builder = LockBuilder::create('users')->accessShare();

        $ast = $builder->toAst();

        static::assertSame(LockMode::ACCESS_SHARE->value, $ast->getMode());
    }

    public function test_lock_exclusive_mode(): void
    {
        $builder = LockBuilder::create('users')->exclusive();

        $ast = $builder->toAst();

        static::assertSame(LockMode::EXCLUSIVE->value, $ast->getMode());
    }

    public function test_lock_in_mode(): void
    {
        $builder = LockBuilder::create('users')->inMode(LockMode::SHARE);

        $ast = $builder->toAst();

        static::assertSame(LockMode::SHARE->value, $ast->getMode());
    }

    public function test_lock_multiple_tables(): void
    {
        $builder = LockBuilder::create('users', 'orders');

        $ast = $builder->toAst();

        static::assertCount(2, $ast->getRelations());
        static::assertSame('users', $ast->getRelations()[0]->getRangeVar()?->getRelname());
        static::assertSame('orders', $ast->getRelations()[1]->getRangeVar()?->getRelname());
    }

    public function test_lock_nowait(): void
    {
        $builder = LockBuilder::create('users')->exclusive()->nowait();

        $ast = $builder->toAst();

        static::assertTrue($ast->getNowait());
    }

    public function test_lock_row_exclusive_mode(): void
    {
        $builder = LockBuilder::create('users')->rowExclusive();

        $ast = $builder->toAst();

        static::assertSame(LockMode::ROW_EXCLUSIVE->value, $ast->getMode());
    }

    public function test_lock_row_share_mode(): void
    {
        $builder = LockBuilder::create('users')->rowShare();

        $ast = $builder->toAst();

        static::assertSame(LockMode::ROW_SHARE->value, $ast->getMode());
    }

    public function test_lock_share_mode(): void
    {
        $builder = LockBuilder::create('users')->share();

        $ast = $builder->toAst();

        static::assertSame(LockMode::SHARE->value, $ast->getMode());
    }

    public function test_lock_share_row_exclusive_mode(): void
    {
        $builder = LockBuilder::create('users')->shareRowExclusive();

        $ast = $builder->toAst();

        static::assertSame(LockMode::SHARE_ROW_EXCLUSIVE->value, $ast->getMode());
    }

    public function test_lock_share_update_exclusive_mode(): void
    {
        $builder = LockBuilder::create('users')->shareUpdateExclusive();

        $ast = $builder->toAst();

        static::assertSame(LockMode::SHARE_UPDATE_EXCLUSIVE->value, $ast->getMode());
    }

    public function test_lock_table_with_schema(): void
    {
        $builder = LockBuilder::create('public.users');

        $ast = $builder->toAst();

        static::assertSame('public', $ast->getRelations()[0]->getRangeVar()?->getSchemaname());
        static::assertSame('users', $ast->getRelations()[0]->getRangeVar()?->getRelname());
    }
}
