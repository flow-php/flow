<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\DropTable;

use Flow\PgQuery\Protobuf\AST\{DropBehavior, DropStmt, ObjectType};
use Flow\PgQuery\QueryBuilder\Schema\DropTable\DropTableBuilder;
use PHPUnit\Framework\TestCase;

final class DropTableBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_drop_multiple_tables() : void
    {
        $builder = DropTableBuilder::create('users', 'orders', 'products');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertCount(3, $ast->getObjects());
    }

    public function test_drop_table_cascade() : void
    {
        $builder = DropTableBuilder::create('users')
            ->cascade();

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_table_if_exists() : void
    {
        $builder = DropTableBuilder::create('users')
            ->ifExists();

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertTrue($ast->getMissingOk());
    }

    public function test_drop_table_if_exists_cascade() : void
    {
        $builder = DropTableBuilder::create('users')
            ->ifExists()
            ->cascade();

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertTrue($ast->getMissingOk());
        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_table_restrict() : void
    {
        $builder = DropTableBuilder::create('users')
            ->restrict();

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_drop_table_with_schema() : void
    {
        $builder = DropTableBuilder::create('public.users');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertCount(1, $ast->getObjects());
        self::assertCount(2, $ast->getObjects()[0]->getList()->getItems());
        self::assertSame('public', $ast->getObjects()[0]->getList()->getItems()[0]->getString()->getSval());
        self::assertSame('users', $ast->getObjects()[0]->getList()->getItems()[1]->getString()->getSval());
    }

    public function test_immutability() : void
    {
        $original = DropTableBuilder::create('users');
        $modified = $original->ifExists();

        self::assertFalse($original->toAst()->getMissingOk());
        self::assertTrue($modified->toAst()->getMissingOk());
    }

    public function test_simple_drop_table() : void
    {
        $builder = DropTableBuilder::create('users');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_TABLE, $ast->getRemoveType());
        self::assertCount(1, $ast->getObjects());
        self::assertSame('users', $ast->getObjects()[0]->getList()->getItems()[0]->getString()->getSval());
    }
}
