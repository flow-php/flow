<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\DropTable;

use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\QueryBuilder\Schema\DropTable\DropTableBuilder;
use PHPUnit\Framework\TestCase;

final class DropTableBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_drop_multiple_tables(): void
    {
        $builder = DropTableBuilder::create('users', 'orders', 'products');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertCount(3, $ast->getObjects());
    }

    public function test_drop_table_cascade(): void
    {
        $builder = DropTableBuilder::create('users')->cascade();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_table_if_exists(): void
    {
        $builder = DropTableBuilder::create('users')->ifExists();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
    }

    public function test_drop_table_if_exists_cascade(): void
    {
        $builder = DropTableBuilder::create('users')->ifExists()->cascade();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_table_restrict(): void
    {
        $builder = DropTableBuilder::create('users')->restrict();

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_drop_table_with_schema(): void
    {
        $builder = DropTableBuilder::create('public.users');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertCount(1, $ast->getObjects());
        static::assertCount(2, $ast->getObjects()[0]->getList()->getItems());
        static::assertSame('public', $ast->getObjects()[0]->getList()->getItems()[0]->getString()->getSval());
        static::assertSame('users', $ast->getObjects()[0]->getList()->getItems()[1]->getString()->getSval());
    }

    public function test_immutability(): void
    {
        $original = DropTableBuilder::create('users');
        $modified = $original->ifExists();

        static::assertFalse($original->toAst()->getMissingOk());
        static::assertTrue($modified->toAst()->getMissingOk());
    }

    public function test_simple_drop_table(): void
    {
        $builder = DropTableBuilder::create('users');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_TABLE, $ast->getRemoveType());
        static::assertCount(1, $ast->getObjects());
        static::assertSame('users', $ast->getObjects()[0]->getList()->getItems()[0]->getString()->getSval());
    }
}
