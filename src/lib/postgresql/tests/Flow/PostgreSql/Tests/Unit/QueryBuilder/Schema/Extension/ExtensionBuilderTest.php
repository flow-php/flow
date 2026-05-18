<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Extension;

use Flow\PostgreSql\Protobuf\AST\AlterExtensionContentsStmt;
use Flow\PostgreSql\Protobuf\AST\AlterExtensionStmt;
use Flow\PostgreSql\Protobuf\AST\CreateExtensionStmt;
use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\QueryBuilder\Schema\Extension\AlterExtensionBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Extension\CreateExtensionBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Extension\DropExtensionBuilder;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;

final class ExtensionBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_alter_extension_add_function_ast_type(): void
    {
        $builder = AlterExtensionBuilder::create('postgis')->addFunction('ST_Distance');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterExtensionContentsStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_FUNCTION, $ast->getObjtype());
        static::assertSame(1, $ast->getAction());
    }

    public function test_alter_extension_add_function_to_sql(): void
    {
        static::assertSame(
            'ALTER EXTENSION postgis ADD FUNCTION st_distance',
            alter()->extension('postgis')->addFunction('st_distance')->toSql(),
        );
    }

    public function test_alter_extension_add_table_ast_type(): void
    {
        $builder = AlterExtensionBuilder::create('postgis')->addTable('spatial_ref_sys');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterExtensionContentsStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        static::assertSame(1, $ast->getAction());
    }

    public function test_alter_extension_add_table_to_sql(): void
    {
        static::assertSame(
            'ALTER EXTENSION postgis ADD TABLE spatial_ref_sys',
            alter()->extension('postgis')->addTable('spatial_ref_sys')->toSql(),
        );
    }

    public function test_alter_extension_drop_function_ast_type(): void
    {
        $builder = AlterExtensionBuilder::create('postgis')->dropFunction('ST_Distance');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterExtensionContentsStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_FUNCTION, $ast->getObjtype());
        static::assertSame(-1, $ast->getAction());
    }

    public function test_alter_extension_drop_function_to_sql(): void
    {
        static::assertSame(
            'ALTER EXTENSION postgis DROP FUNCTION st_distance',
            alter()->extension('postgis')->dropFunction('st_distance')->toSql(),
        );
    }

    public function test_alter_extension_drop_table_ast_type(): void
    {
        $builder = AlterExtensionBuilder::create('postgis')->dropTable('spatial_ref_sys');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterExtensionContentsStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_TABLE, $ast->getObjtype());
        static::assertSame(-1, $ast->getAction());
    }

    public function test_alter_extension_drop_table_to_sql(): void
    {
        static::assertSame(
            'ALTER EXTENSION postgis DROP TABLE spatial_ref_sys',
            alter()->extension('postgis')->dropTable('spatial_ref_sys')->toSql(),
        );
    }

    public function test_alter_extension_update_ast_type(): void
    {
        $builder = AlterExtensionBuilder::create('postgis')->update();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterExtensionStmt::class, $ast);
    }

    public function test_alter_extension_update_sets_name(): void
    {
        $builder = AlterExtensionBuilder::create('postgis')->update();

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterExtensionStmt::class, $ast);
        static::assertSame('postgis', $ast->getExtname());
    }

    public function test_alter_extension_update_to_sets_version(): void
    {
        $builder = AlterExtensionBuilder::create('postgis')->updateTo('3.1');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterExtensionStmt::class, $ast);

        $options = $ast->getOptions();
        static::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('new_version', $defElem->getDefname());
    }

    public function test_alter_extension_update_to_sql(): void
    {
        static::assertSame('ALTER EXTENSION postgis UPDATE', alter()->extension('postgis')->update()->toSql());
    }

    public function test_alter_extension_update_to_version_to_sql(): void
    {
        static::assertSame(
            'ALTER EXTENSION postgis UPDATE TO "3.1"',
            alter()->extension('postgis')->updateTo('3.1')->toSql(),
        );
    }

    public function test_create_extension_ast_type(): void
    {
        $builder = CreateExtensionBuilder::create('uuid-ossp');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateExtensionStmt::class, $ast);
    }

    public function test_create_extension_cascade_sets_option(): void
    {
        $builder = CreateExtensionBuilder::create('postgis')->cascade();

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        static::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('cascade', $defElem->getDefname());
    }

    public function test_create_extension_full_to_sql(): void
    {
        static::assertSame(
            'CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public VERSION "3.0" CASCADE',
            create()->extension('postgis')->ifNotExists()->schema('public')->version('3.0')->cascade()->toSql(),
        );
    }

    public function test_create_extension_if_not_exists_sets_flag(): void
    {
        $builder = CreateExtensionBuilder::create('postgis')->ifNotExists();

        $ast = $builder->toAst();

        static::assertTrue($ast->getIfNotExists());
    }

    public function test_create_extension_if_not_exists_to_sql(): void
    {
        static::assertSame(
            'CREATE EXTENSION IF NOT EXISTS postgis',
            create()->extension('postgis')->ifNotExists()->toSql(),
        );
    }

    public function test_create_extension_immutability(): void
    {
        $original = CreateExtensionBuilder::create('postgis');
        $modified = $original->ifNotExists();

        static::assertFalse($original->toAst()->getIfNotExists());
        static::assertTrue($modified->toAst()->getIfNotExists());
    }

    public function test_create_extension_schema_sets_option(): void
    {
        $builder = CreateExtensionBuilder::create('postgis')->schema('public');

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        static::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('schema', $defElem->getDefname());
    }

    public function test_create_extension_sets_name(): void
    {
        $builder = CreateExtensionBuilder::create('postgis');

        $ast = $builder->toAst();

        static::assertSame('postgis', $ast->getExtname());
    }

    public function test_create_extension_simple_to_sql(): void
    {
        static::assertSame('CREATE EXTENSION "uuid-ossp"', create()->extension('uuid-ossp')->toSql());
    }

    public function test_create_extension_version_sets_option(): void
    {
        $builder = CreateExtensionBuilder::create('postgis')->version('3.0');

        $ast = $builder->toAst();
        $options = $ast->getOptions();

        static::assertCount(1, $options);

        $defElem = $options[0]->getDefElem();
        static::assertNotNull($defElem);
        static::assertSame('new_version', $defElem->getDefname());
    }

    public function test_create_extension_with_cascade_to_sql(): void
    {
        static::assertSame('CREATE EXTENSION postgis CASCADE', create()->extension('postgis')->cascade()->toSql());
    }

    public function test_create_extension_with_schema_to_sql(): void
    {
        static::assertSame(
            'CREATE EXTENSION postgis SCHEMA public',
            create()->extension('postgis')->schema('public')->toSql(),
        );
    }

    public function test_create_extension_with_version_to_sql(): void
    {
        static::assertSame(
            'CREATE EXTENSION postgis VERSION "3.0"',
            create()->extension('postgis')->version('3.0')->toSql(),
        );
    }

    public function test_drop_extension_ast_type(): void
    {
        $builder = DropExtensionBuilder::create('postgis');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_EXTENSION, $ast->getRemoveType());
    }

    public function test_drop_extension_cascade_sets_behavior(): void
    {
        $builder = DropExtensionBuilder::create('postgis')->cascade();

        $ast = $builder->toAst();

        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_extension_cascade_to_sql(): void
    {
        static::assertSame('DROP EXTENSION postgis CASCADE', drop()->extension('postgis')->cascade()->toSql());
    }

    public function test_drop_extension_if_exists_cascade_to_sql(): void
    {
        static::assertSame(
            'DROP EXTENSION IF EXISTS postgis CASCADE',
            drop()->extension('postgis')->ifExists()->cascade()->toSql(),
        );
    }

    public function test_drop_extension_if_exists_sets_flag(): void
    {
        $builder = DropExtensionBuilder::create('postgis')->ifExists();

        $ast = $builder->toAst();

        static::assertTrue($ast->getMissingOk());
    }

    public function test_drop_extension_if_exists_to_sql(): void
    {
        static::assertSame('DROP EXTENSION IF EXISTS postgis', drop()->extension('postgis')->ifExists()->toSql());
    }

    public function test_drop_extension_immutability(): void
    {
        $original = DropExtensionBuilder::create('postgis');
        $modified = $original->ifExists();

        static::assertFalse($original->toAst()->getMissingOk());
        static::assertTrue($modified->toAst()->getMissingOk());
    }

    public function test_drop_extension_multiple_extensions(): void
    {
        $builder = DropExtensionBuilder::create('postgis', 'pg_trgm', 'uuid-ossp');

        $ast = $builder->toAst();

        static::assertCount(3, $ast->getObjects());
    }

    public function test_drop_extension_multiple_to_sql(): void
    {
        static::assertSame(
            'DROP EXTENSION postgis, pg_trgm, "uuid-ossp"',
            drop()->extension('postgis', 'pg_trgm', 'uuid-ossp')->toSql(),
        );
    }

    public function test_drop_extension_restrict_sets_behavior(): void
    {
        $builder = DropExtensionBuilder::create('postgis')->restrict();

        $ast = $builder->toAst();

        static::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_drop_extension_restrict_to_sql(): void
    {
        static::assertSame('DROP EXTENSION postgis', drop()->extension('postgis')->restrict()->toSql());
    }

    public function test_drop_extension_sets_name(): void
    {
        $builder = DropExtensionBuilder::create('postgis');

        $ast = $builder->toAst();

        static::assertCount(1, $ast->getObjects());
    }

    public function test_drop_extension_simple_to_sql(): void
    {
        static::assertSame('DROP EXTENSION postgis', drop()->extension('postgis')->toSql());
    }
}
