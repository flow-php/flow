<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PgQuery\Protobuf\AST\{AlterTableStmt, AlterTableType, ObjectType, RenameStmt};
use Flow\PgQuery\QueryBuilder\Schema\Index\AlterIndex\{AlterIndexBuilder};
use PHPUnit\Framework\TestCase;

final class AlterIndexBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_if_exists_sets_flag_for_rename() : void
    {
        $builder = AlterIndexBuilder::create('idx_old')
            ->ifExists()
            ->renameTo('idx_new');

        $ast = $builder->toAst();

        self::assertInstanceOf(RenameStmt::class, $ast);
        self::assertTrue($ast->getMissingOk());
    }

    public function test_if_exists_sets_flag_for_tablespace() : void
    {
        $builder = AlterIndexBuilder::create('idx_users_email')
            ->ifExists()
            ->setTablespace('fast_storage');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterTableStmt::class, $ast);
        self::assertTrue($ast->getMissingOk());
    }

    public function test_immutability() : void
    {
        $original = AlterIndexBuilder::create('idx_old');
        $modified = $original->ifExists();

        $renamedOriginal = $original->renameTo('idx_new');
        $renamedModified = $modified->renameTo('idx_new');

        self::assertFalse($renamedOriginal->toAst()->getMissingOk());
        self::assertTrue($renamedModified->toAst()->getMissingOk());
    }

    public function test_rename_creates_rename_stmt() : void
    {
        $builder = AlterIndexBuilder::create('idx_old')
            ->renameTo('idx_new');

        $ast = $builder->toAst();

        self::assertInstanceOf(RenameStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_INDEX, $ast->getRenameType());
        self::assertSame('idx_old', $ast->getRelation()->getRelname());
        self::assertSame('idx_new', $ast->getNewname());
    }

    public function test_rename_with_schema() : void
    {
        $builder = AlterIndexBuilder::create('idx_old', 'public')
            ->renameTo('idx_new');

        $ast = $builder->toAst();

        self::assertInstanceOf(RenameStmt::class, $ast);
        self::assertSame('public', $ast->getRelation()->getSchemaname());
        self::assertSame('idx_old', $ast->getRelation()->getRelname());
    }

    public function test_set_tablespace_creates_alter_table_stmt() : void
    {
        $builder = AlterIndexBuilder::create('idx_users_email')
            ->setTablespace('fast_storage');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterTableStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_INDEX, $ast->getObjtype());
        self::assertSame('idx_users_email', $ast->getRelation()->getRelname());
        self::assertCount(1, $ast->getCmds());
        self::assertSame(AlterTableType::AT_SetTableSpace, $ast->getCmds()[0]->getAlterTableCmd()->getSubtype());
        self::assertSame('fast_storage', $ast->getCmds()[0]->getAlterTableCmd()->getName());
    }

    public function test_set_tablespace_with_schema() : void
    {
        $builder = AlterIndexBuilder::create('idx_users_email', 'public')
            ->setTablespace('fast_storage');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterTableStmt::class, $ast);
        self::assertSame('public', $ast->getRelation()->getSchemaname());
        self::assertSame('idx_users_email', $ast->getRelation()->getRelname());
    }
}
