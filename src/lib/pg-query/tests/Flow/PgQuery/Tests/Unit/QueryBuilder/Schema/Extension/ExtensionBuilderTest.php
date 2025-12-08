<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\Extension;

use Flow\PgQuery\Protobuf\AST\{AlterExtensionContentsStmt, AlterExtensionStmt, CreateExtensionStmt, DropBehavior, DropStmt, ObjectType};
use Flow\PgQuery\QueryBuilder\Schema\Extension\{AlterExtensionBuilder, CreateExtensionBuilder, DropExtensionBuilder};
use PHPUnit\Framework\TestCase;

final class ExtensionBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_alter_extension_add_function_ast_type() : void
    {
        $builder = AlterExtensionBuilder::create('postgis')
            ->addFunction('ST_Distance');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterExtensionContentsStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_FUNCTION, $ast->getObjtype());
        self::assertSame(1, $ast->getAction());
    }

    public function test_alter_extension_add_table_ast_type() : void
    {
        $builder = AlterExtensionBuilder::create('postgis')
            ->addTable('spatial_ref_sys');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterExtensionContentsStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        self::assertSame(1, $ast->getAction());
    }

    public function test_alter_extension_drop_function_ast_type() : void
    {
        $builder = AlterExtensionBuilder::create('postgis')
            ->dropFunction('ST_Distance');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterExtensionContentsStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_FUNCTION, $ast->getObjtype());
        self::assertSame(-1, $ast->getAction());
    }

    public function test_alter_extension_drop_table_ast_type() : void
    {
        $builder = AlterExtensionBuilder::create('postgis')
            ->dropTable('spatial_ref_sys');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterExtensionContentsStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        self::assertSame(-1, $ast->getAction());
    }

    public function test_alter_extension_update_ast_type() : void
    {
        $builder = AlterExtensionBuilder::create('postgis')
            ->update();

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterExtensionStmt::class, $ast);
    }

    public function test_alter_extension_update_sets_name() : void
    {
        $builder = AlterExtensionBuilder::create('postgis')
            ->update();

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterExtensionStmt::class, $ast);
        self::assertSame('postgis', $ast->getExtname());
    }

    public function test_alter_extension_update_to_sets_version() : void
    {
        $builder = AlterExtensionBuilder::create('postgis')
            ->updateTo('3.1');

        $ast = $builder->toAst();

        self::assertInstanceOf(AlterExtensionStmt::class, $ast);

        $options = $ast->getOptions();
        self::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        self::assertNotNull($defElem);
        self::assertSame('new_version', $defElem->getDefname());
    }

    public function test_create_extension_ast_type() : void
    {
        $builder = CreateExtensionBuilder::create('uuid-ossp');

        $ast = $builder->toAst();

        self::assertInstanceOf(CreateExtensionStmt::class, $ast);
    }

    public function test_create_extension_cascade_sets_option() : void
    {
        $builder = CreateExtensionBuilder::create('postgis')
            ->cascade();

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        self::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        self::assertNotNull($defElem);
        self::assertSame('cascade', $defElem->getDefname());
    }

    public function test_create_extension_if_not_exists_sets_flag() : void
    {
        $builder = CreateExtensionBuilder::create('postgis')
            ->ifNotExists();

        $ast = $builder->toAst();

        self::assertTrue($ast->getIfNotExists());
    }

    public function test_create_extension_immutability() : void
    {
        $original = CreateExtensionBuilder::create('postgis');
        $modified = $original->ifNotExists();

        self::assertFalse($original->toAst()->getIfNotExists());
        self::assertTrue($modified->toAst()->getIfNotExists());
    }

    public function test_create_extension_schema_sets_option() : void
    {
        $builder = CreateExtensionBuilder::create('postgis')
            ->schema('public');

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        self::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        self::assertNotNull($defElem);
        self::assertSame('schema', $defElem->getDefname());
    }

    public function test_create_extension_sets_name() : void
    {
        $builder = CreateExtensionBuilder::create('postgis');

        $ast = $builder->toAst();

        self::assertSame('postgis', $ast->getExtname());
    }

    public function test_create_extension_version_sets_option() : void
    {
        $builder = CreateExtensionBuilder::create('postgis')
            ->version('3.0');

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        self::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        self::assertNotNull($defElem);
        self::assertSame('new_version', $defElem->getDefname());
    }

    public function test_drop_extension_ast_type() : void
    {
        $builder = DropExtensionBuilder::create('postgis');

        $ast = $builder->toAst();

        self::assertInstanceOf(DropStmt::class, $ast);
        self::assertSame(ObjectType::OBJECT_EXTENSION, $ast->getRemoveType());
    }

    public function test_drop_extension_cascade_sets_behavior() : void
    {
        $builder = DropExtensionBuilder::create('postgis')
            ->cascade();

        $ast = $builder->toAst();

        self::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_extension_if_exists_sets_flag() : void
    {
        $builder = DropExtensionBuilder::create('postgis')
            ->ifExists();

        $ast = $builder->toAst();

        self::assertTrue($ast->getMissingOk());
    }

    public function test_drop_extension_immutability() : void
    {
        $original = DropExtensionBuilder::create('postgis');
        $modified = $original->ifExists();

        self::assertFalse($original->toAst()->getMissingOk());
        self::assertTrue($modified->toAst()->getMissingOk());
    }

    public function test_drop_extension_multiple_extensions() : void
    {
        $builder = DropExtensionBuilder::create('postgis', 'pg_trgm', 'uuid-ossp');

        $ast = $builder->toAst();

        self::assertCount(3, $ast->getObjects());
    }

    public function test_drop_extension_restrict_sets_behavior() : void
    {
        $builder = DropExtensionBuilder::create('postgis')
            ->restrict();

        $ast = $builder->toAst();

        self::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_drop_extension_sets_name() : void
    {
        $builder = DropExtensionBuilder::create('postgis');

        $ast = $builder->toAst();

        self::assertCount(1, $ast->getObjects());
    }
}
