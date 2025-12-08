<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{alter_schema, create_schema, drop_schema};

final class SchemaBuilderTest extends PGQueryTestCase
{
    public function test_alter_schema_owner() : void
    {
        $builder = alter_schema('my_schema')
            ->ownerTo('new_owner');

        $this->assertAlterSchemaOwnerQuery(
            $builder,
            'ALTER SCHEMA my_schema OWNER TO new_owner'
        );
    }

    public function test_alter_schema_rename() : void
    {
        $builder = alter_schema('old_schema')
            ->renameTo('new_schema');

        $this->assertAlterSchemaRenameQuery(
            $builder,
            'ALTER SCHEMA old_schema RENAME TO new_schema'
        );
    }

    public function test_create_schema_if_not_exists() : void
    {
        $builder = create_schema('my_schema')
            ->ifNotExists();

        $this->assertCreateSchemaQuery(
            $builder,
            'CREATE SCHEMA IF NOT EXISTS my_schema'
        );
    }

    public function test_create_schema_if_not_exists_with_authorization() : void
    {
        $builder = create_schema('my_schema')
            ->ifNotExists()
            ->authorization('admin_user');

        $this->assertCreateSchemaQuery(
            $builder,
            'CREATE SCHEMA IF NOT EXISTS my_schema AUTHORIZATION admin_user'
        );
    }

    public function test_create_schema_simple() : void
    {
        $builder = create_schema('my_schema');

        $this->assertCreateSchemaQuery(
            $builder,
            'CREATE SCHEMA my_schema'
        );
    }

    public function test_create_schema_with_authorization() : void
    {
        $builder = create_schema('my_schema')
            ->authorization('admin_user');

        $this->assertCreateSchemaQuery(
            $builder,
            'CREATE SCHEMA my_schema AUTHORIZATION admin_user'
        );
    }

    public function test_drop_schema_cascade() : void
    {
        $builder = drop_schema('my_schema')
            ->cascade();

        $this->assertDropSchemaQuery(
            $builder,
            'DROP SCHEMA my_schema CASCADE'
        );
    }

    public function test_drop_schema_if_exists() : void
    {
        $builder = drop_schema('my_schema')
            ->ifExists();

        $this->assertDropSchemaQuery(
            $builder,
            'DROP SCHEMA IF EXISTS my_schema'
        );
    }

    public function test_drop_schema_if_exists_cascade() : void
    {
        $builder = drop_schema('my_schema')
            ->ifExists()
            ->cascade();

        $this->assertDropSchemaQuery(
            $builder,
            'DROP SCHEMA IF EXISTS my_schema CASCADE'
        );
    }

    public function test_drop_schema_multiple() : void
    {
        $builder = drop_schema('schema1', 'schema2');

        $this->assertDropSchemaQuery(
            $builder,
            'DROP SCHEMA schema1, schema2'
        );
    }

    public function test_drop_schema_restrict() : void
    {
        $builder = drop_schema('my_schema')
            ->restrict();

        $this->assertDropSchemaQuery(
            $builder,
            'DROP SCHEMA my_schema'
        );
    }

    public function test_drop_schema_simple() : void
    {
        $builder = drop_schema('my_schema');

        $this->assertDropSchemaQuery(
            $builder,
            'DROP SCHEMA my_schema'
        );
    }
}
