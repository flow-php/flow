<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{alter, create, drop};

final class ExtensionBuilderTest extends PGQueryTestCase
{
    public function test_alter_extension_add_function() : void
    {
        $builder = alter()->extension('postgis')
            ->addFunction('st_distance');

        $this->assertAlterExtensionQuery(
            $builder,
            'ALTER EXTENSION postgis ADD FUNCTION st_distance'
        );
    }

    public function test_alter_extension_add_table() : void
    {
        $builder = alter()->extension('postgis')
            ->addTable('spatial_ref_sys');

        $this->assertAlterExtensionQuery(
            $builder,
            'ALTER EXTENSION postgis ADD TABLE spatial_ref_sys'
        );
    }

    public function test_alter_extension_drop_function() : void
    {
        $builder = alter()->extension('postgis')
            ->dropFunction('st_distance');

        $this->assertAlterExtensionQuery(
            $builder,
            'ALTER EXTENSION postgis DROP FUNCTION st_distance'
        );
    }

    public function test_alter_extension_drop_table() : void
    {
        $builder = alter()->extension('postgis')
            ->dropTable('spatial_ref_sys');

        $this->assertAlterExtensionQuery(
            $builder,
            'ALTER EXTENSION postgis DROP TABLE spatial_ref_sys'
        );
    }

    public function test_alter_extension_update() : void
    {
        $builder = alter()->extension('postgis')
            ->update();

        $this->assertAlterExtensionQuery(
            $builder,
            'ALTER EXTENSION postgis UPDATE'
        );
    }

    public function test_alter_extension_update_to() : void
    {
        $builder = alter()->extension('postgis')
            ->updateTo('3.1');

        $this->assertAlterExtensionQuery(
            $builder,
            'ALTER EXTENSION postgis UPDATE TO "3.1"'
        );
    }

    public function test_create_extension_full() : void
    {
        $builder = create()->extension('postgis')
            ->ifNotExists()
            ->schema('public')
            ->version('3.0')
            ->cascade();

        $this->assertCreateExtensionQuery(
            $builder,
            'CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public VERSION "3.0" CASCADE'
        );
    }

    public function test_create_extension_if_not_exists() : void
    {
        $builder = create()->extension('postgis')
            ->ifNotExists();

        $this->assertCreateExtensionQuery(
            $builder,
            'CREATE EXTENSION IF NOT EXISTS postgis'
        );
    }

    public function test_create_extension_simple() : void
    {
        $builder = create()->extension('uuid-ossp');

        $this->assertCreateExtensionQuery(
            $builder,
            'CREATE EXTENSION "uuid-ossp"'
        );
    }

    public function test_create_extension_with_cascade() : void
    {
        $builder = create()->extension('postgis')
            ->cascade();

        $this->assertCreateExtensionQuery(
            $builder,
            'CREATE EXTENSION postgis CASCADE'
        );
    }

    public function test_create_extension_with_schema() : void
    {
        $builder = create()->extension('postgis')
            ->schema('public');

        $this->assertCreateExtensionQuery(
            $builder,
            'CREATE EXTENSION postgis SCHEMA public'
        );
    }

    public function test_create_extension_with_version() : void
    {
        $builder = create()->extension('postgis')
            ->version('3.0');

        $this->assertCreateExtensionQuery(
            $builder,
            'CREATE EXTENSION postgis VERSION "3.0"'
        );
    }

    public function test_drop_extension_cascade() : void
    {
        $builder = drop()->extension('postgis')
            ->cascade();

        $this->assertDropExtensionQuery(
            $builder,
            'DROP EXTENSION postgis CASCADE'
        );
    }

    public function test_drop_extension_if_exists() : void
    {
        $builder = drop()->extension('postgis')
            ->ifExists();

        $this->assertDropExtensionQuery(
            $builder,
            'DROP EXTENSION IF EXISTS postgis'
        );
    }

    public function test_drop_extension_if_exists_cascade() : void
    {
        $builder = drop()->extension('postgis')
            ->ifExists()
            ->cascade();

        $this->assertDropExtensionQuery(
            $builder,
            'DROP EXTENSION IF EXISTS postgis CASCADE'
        );
    }

    public function test_drop_extension_multiple() : void
    {
        $builder = drop()->extension('postgis', 'pg_trgm', 'uuid-ossp');

        $this->assertDropExtensionQuery(
            $builder,
            'DROP EXTENSION postgis, pg_trgm, "uuid-ossp"'
        );
    }

    public function test_drop_extension_restrict() : void
    {
        $builder = drop()->extension('postgis')
            ->restrict();

        $this->assertDropExtensionQuery(
            $builder,
            'DROP EXTENSION postgis'
        );
    }

    public function test_drop_extension_simple() : void
    {
        $builder = drop()->extension('postgis');

        $this->assertDropExtensionQuery(
            $builder,
            'DROP EXTENSION postgis'
        );
    }
}
