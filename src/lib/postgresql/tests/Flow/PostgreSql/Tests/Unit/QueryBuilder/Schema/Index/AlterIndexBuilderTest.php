<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTableType;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\QueryBuilder\Schema\Index\AlterIndex\AlterIndexBuilder;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_instance_of;

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
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('idx_old', $relation->getRelname());
        static::assertSame('idx_new', $ast->getNewname());
    }

    public function test_rename_with_schema(): void
    {
        $builder = AlterIndexBuilder::create('idx_old', 'public')->renameTo('idx_new');

        $ast = $builder->toAst();

        static::assertInstanceOf(RenameStmt::class, $ast);
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('public', $relation->getSchemaname());
        static::assertSame('idx_old', $relation->getRelname());
    }

    public function test_set_tablespace_creates_alter_table_stmt(): void
    {
        $builder = AlterIndexBuilder::create('idx_users_email')->setTablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_INDEX, $ast->getObjtype());
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('idx_users_email', $relation->getRelname());
        $cmds = $ast->getCmds();
        static::assertCount(1, $cmds);
        $alterTableCmd = type_instance_of(Node::class)->assert($cmds[0])->getAlterTableCmd();
        static::assertNotNull($alterTableCmd);
        static::assertSame(AlterTableType::AT_SetTableSpace, $alterTableCmd->getSubtype());
        static::assertSame('fast_storage', $alterTableCmd->getName());
    }

    public function test_set_tablespace_with_schema(): void
    {
        $builder = AlterIndexBuilder::create('idx_users_email', 'public')->setTablespace('fast_storage');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterTableStmt::class, $ast);
        $relation = $ast->getRelation();
        static::assertNotNull($relation);
        static::assertSame('public', $relation->getSchemaname());
        static::assertSame('idx_users_email', $relation->getRelname());
    }
}
