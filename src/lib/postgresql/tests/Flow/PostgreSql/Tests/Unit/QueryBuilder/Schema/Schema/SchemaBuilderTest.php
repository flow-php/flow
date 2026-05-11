<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Schema;

use Flow\PostgreSql\Protobuf\AST\AlterOwnerStmt;
use Flow\PostgreSql\Protobuf\AST\CreateSchemaStmt;
use Flow\PostgreSql\Protobuf\AST\DropBehavior;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\ObjectType;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;
use Flow\PostgreSql\Protobuf\AST\RoleSpecType;
use Flow\PostgreSql\QueryBuilder\Schema\Schema\AlterSchemaBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Schema\CreateSchemaBuilder;
use Flow\PostgreSql\QueryBuilder\Schema\Schema\DropSchemaBuilder;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;

final class SchemaBuilderTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_alter_schema_owner_ast_type(): void
    {
        $builder = AlterSchemaBuilder::create('my_schema')->ownerTo('new_owner');

        $ast = $builder->toAst();

        static::assertInstanceOf(AlterOwnerStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_SCHEMA, $ast->getObjectType());
    }

    public function test_alter_schema_owner_sets_new_owner(): void
    {
        $builder = AlterSchemaBuilder::create('my_schema')->ownerTo('admin_user');

        $ast = $builder->toAst();
        $newOwner = $ast->getNewowner();

        static::assertNotNull($newOwner);
        static::assertSame(RoleSpecType::ROLESPEC_CSTRING, $newOwner->getRoletype());
        static::assertSame('admin_user', $newOwner->getRolename());
    }

    public function test_alter_schema_owner_to_sql(): void
    {
        static::assertSame(
            'ALTER SCHEMA my_schema OWNER TO new_owner',
            alter()->schema('my_schema')->ownerTo('new_owner')->toSql(),
        );
    }

    public function test_alter_schema_rename_ast_type(): void
    {
        $builder = AlterSchemaBuilder::create('old_schema')->renameTo('new_schema');

        $ast = $builder->toAst();

        static::assertInstanceOf(RenameStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_SCHEMA, $ast->getRenameType());
    }

    public function test_alter_schema_rename_sets_names(): void
    {
        $builder = AlterSchemaBuilder::create('old_schema')->renameTo('new_schema');

        $ast = $builder->toAst();

        static::assertSame('old_schema', $ast->getSubname());
        static::assertSame('new_schema', $ast->getNewname());
    }

    public function test_alter_schema_rename_to_sql(): void
    {
        static::assertSame(
            'ALTER SCHEMA old_schema RENAME TO new_schema',
            alter()->schema('old_schema')->renameTo('new_schema')->toSql(),
        );
    }

    public function test_create_schema_ast_type(): void
    {
        $builder = CreateSchemaBuilder::create('my_schema');

        $ast = $builder->toAst();

        static::assertInstanceOf(CreateSchemaStmt::class, $ast);
    }

    public function test_create_schema_authorization_sets_role(): void
    {
        $builder = CreateSchemaBuilder::create('my_schema')->authorization('admin_user');

        $ast = $builder->toAst();
        $authRole = $ast->getAuthrole();

        static::assertNotNull($authRole);
        static::assertSame(RoleSpecType::ROLESPEC_CSTRING, $authRole->getRoletype());
        static::assertSame('admin_user', $authRole->getRolename());
    }

    public function test_create_schema_if_not_exists_sets_flag(): void
    {
        $builder = CreateSchemaBuilder::create('my_schema')->ifNotExists();

        $ast = $builder->toAst();

        static::assertTrue($ast->getIfNotExists());
    }

    public function test_create_schema_if_not_exists_to_sql(): void
    {
        static::assertSame(
            'CREATE SCHEMA IF NOT EXISTS my_schema',
            create()->schema('my_schema')->ifNotExists()->toSql(),
        );
    }

    public function test_create_schema_if_not_exists_with_authorization_to_sql(): void
    {
        static::assertSame(
            'CREATE SCHEMA IF NOT EXISTS my_schema AUTHORIZATION admin_user',
            create()->schema('my_schema')->ifNotExists()->authorization('admin_user')->toSql(),
        );
    }

    public function test_create_schema_immutability(): void
    {
        $original = CreateSchemaBuilder::create('my_schema');
        $modified = $original->ifNotExists();

        static::assertFalse($original->toAst()->getIfNotExists());
        static::assertTrue($modified->toAst()->getIfNotExists());
    }

    public function test_create_schema_sets_name(): void
    {
        $builder = CreateSchemaBuilder::create('my_schema');

        $ast = $builder->toAst();

        static::assertSame('my_schema', $ast->getSchemaname());
    }

    public function test_create_schema_simple_to_sql(): void
    {
        static::assertSame('CREATE SCHEMA my_schema', create()->schema('my_schema')->toSql());
    }

    public function test_create_schema_with_authorization_to_sql(): void
    {
        static::assertSame(
            'CREATE SCHEMA my_schema AUTHORIZATION admin_user',
            create()->schema('my_schema')->authorization('admin_user')->toSql(),
        );
    }

    public function test_drop_schema_ast_type(): void
    {
        $builder = DropSchemaBuilder::create('my_schema');

        $ast = $builder->toAst();

        static::assertInstanceOf(DropStmt::class, $ast);
        static::assertSame(ObjectType::OBJECT_SCHEMA, $ast->getRemoveType());
    }

    public function test_drop_schema_cascade_sets_behavior(): void
    {
        $builder = DropSchemaBuilder::create('my_schema')->cascade();

        $ast = $builder->toAst();

        static::assertSame(DropBehavior::DROP_CASCADE, $ast->getBehavior());
    }

    public function test_drop_schema_cascade_to_sql(): void
    {
        static::assertSame('DROP SCHEMA my_schema CASCADE', drop()->schema('my_schema')->cascade()->toSql());
    }

    public function test_drop_schema_if_exists_cascade_to_sql(): void
    {
        static::assertSame(
            'DROP SCHEMA IF EXISTS my_schema CASCADE',
            drop()->schema('my_schema')->ifExists()->cascade()->toSql(),
        );
    }

    public function test_drop_schema_if_exists_sets_flag(): void
    {
        $builder = DropSchemaBuilder::create('my_schema')->ifExists();

        $ast = $builder->toAst();

        static::assertTrue($ast->getMissingOk());
    }

    public function test_drop_schema_if_exists_to_sql(): void
    {
        static::assertSame('DROP SCHEMA IF EXISTS my_schema', drop()->schema('my_schema')->ifExists()->toSql());
    }

    public function test_drop_schema_immutability(): void
    {
        $original = DropSchemaBuilder::create('my_schema');
        $modified = $original->ifExists();

        static::assertFalse($original->toAst()->getMissingOk());
        static::assertTrue($modified->toAst()->getMissingOk());
    }

    public function test_drop_schema_multiple_schemas(): void
    {
        $builder = DropSchemaBuilder::create('schema1', 'schema2', 'schema3');

        $ast = $builder->toAst();

        static::assertCount(3, $ast->getObjects());
    }

    public function test_drop_schema_multiple_to_sql(): void
    {
        static::assertSame('DROP SCHEMA schema1, schema2', drop()->schema('schema1', 'schema2')->toSql());
    }

    public function test_drop_schema_restrict_sets_behavior(): void
    {
        $builder = DropSchemaBuilder::create('my_schema')->restrict();

        $ast = $builder->toAst();

        static::assertSame(DropBehavior::DROP_RESTRICT, $ast->getBehavior());
    }

    public function test_drop_schema_restrict_to_sql(): void
    {
        static::assertSame('DROP SCHEMA my_schema', drop()->schema('my_schema')->restrict()->toSql());
    }

    public function test_drop_schema_simple_to_sql(): void
    {
        static::assertSame('DROP SCHEMA my_schema', drop()->schema('my_schema')->toSql());
    }
}
