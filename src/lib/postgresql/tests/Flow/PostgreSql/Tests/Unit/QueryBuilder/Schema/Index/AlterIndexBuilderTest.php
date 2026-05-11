<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTableType;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\QueryBuilder\Schema\Index\AlterIndex\AlterIndexBuilder;
use PHPUnit\Framework\TestCase;

final class AlterIndexBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_if_exists_sets_flag_for_rename(): void
    {
        $builder = AlterIndexBuilder::create('idx_old')->ifExists()->renameTo('idx_new');

        $ast = $builder->toAst();

        static::assertInstanceOf(RenameStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
    }

    public function test_if_exists_sets_flag_for_tablespace(): void
    {
        $builder = AlterIndexBuilder::create('idx_users_email')->ifExists()->setTablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertTrue($ast->getMissingOk());
    }

    public function test_immutability(): void
    {
        $original = AlterIndexBuilder::create('idx_old');
        $modified = $original->ifExists();

        $renamedOriginal = $original->renameTo('idx_new');
        $renamedModified = $modified->renameTo('idx_new');

        static::assertFalse($renamedOriginal->toAst()->getMissingOk());
        static::assertTrue($renamedModified->toAst()->getMissingOk());
    }

    public function test_rename_creates_rename_stmt(): void
    {
        $builder = AlterIndexBuilder::create('idx_old')->renameTo('idx_new');

        $ast = $builder->toAst();

        static::assertInstanceOf(RenameStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_INDEX, $ast->getRenameType());
        static::assertSame('idx_old', $ast->getRelation()->getRelname());
        static::assertSame('idx_new', $ast->getNewname());
    }

    public function test_rename_with_schema(): void
    {
        $builder = AlterIndexBuilder::create('idx_old', 'public')->renameTo('idx_new');

        $ast = $builder->toAst();

        static::assertInstanceOf(RenameStmt::class, $ast);
        static::assertSame('public', $ast->getRelation()->getSchemaname());
        static::assertSame('idx_old', $ast->getRelation()->getRelname());
    }

    public function test_set_tablespace_creates_alter_table_stmt(): void
    {
        $builder = AlterIndexBuilder::create('idx_users_email')->setTablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_INDEX, $ast->getObjtype());
        static::assertSame('idx_users_email', $ast->getRelation()->getRelname());
        static::assertCount(1, $ast->getCmds());
        static::assertSame(AlterTableType::AT_SetTableSpace, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        static::assertSame('fast_storage', $ast->getCmds()[0]->getAlterTableCmd()->getName());
    }

    public function test_set_tablespace_with_schema(): void
    {
        $builder = AlterIndexBuilder::create('idx_users_email', 'public')->setTablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame('public', $ast->getRelation()->getSchemaname());
        static::assertSame('idx_users_email', $ast->getRelation()->getRelname());
    }
}
