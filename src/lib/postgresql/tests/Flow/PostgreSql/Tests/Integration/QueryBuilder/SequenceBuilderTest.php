<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder;

use function Flow\PostgreSql\DSL\{
    alter,
    create,
    drop
};

final class SequenceBuilderTest extends PGQueryTestCase
{
    public function test_alter_sequence_as_type() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->asType('smallint');

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq AS smallint'
        );
    }

    public function test_alter_sequence_cache() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->cache(20);

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq CACHE 20'
        );
    }

    public function test_alter_sequence_cycle() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->cycle();

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq CYCLE'
        );
    }

    public function test_alter_sequence_if_exists() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->withIfExists()
            ->incrementBy(10);

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE IF EXISTS user_id_seq INCREMENT 10'
        );
    }

    public function test_alter_sequence_increment_by() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->incrementBy(10);

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq INCREMENT 10'
        );
    }

    public function test_alter_sequence_max_value() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->maxValue(9999999);

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq MAXVALUE 9999999'
        );
    }

    public function test_alter_sequence_min_value() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->minValue(1);

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq MINVALUE 1'
        );
    }

    public function test_alter_sequence_multiple_options() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->incrementBy(10)
            ->minValue(1)
            ->maxValue(1000000)
            ->cache(5);

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq INCREMENT 10 MINVALUE 1 MAXVALUE 1000000 CACHE 5'
        );
    }

    public function test_alter_sequence_no_cycle() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->noCycle();

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq NO CYCLE'
        );
    }

    public function test_alter_sequence_no_max_value() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->noMaxValue();

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq NO MAXVALUE'
        );
    }

    public function test_alter_sequence_no_min_value() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->noMinValue();

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq NO MINVALUE'
        );
    }

    public function test_alter_sequence_owned_by() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->ownedBy('users', 'id');

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq OWNED BY users.id'
        );
    }

    public function test_alter_sequence_owned_by_none() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->ownedByNone();

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq OWNED BY "none"'
        );
    }

    public function test_alter_sequence_owner_to() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->ownerTo('new_owner');

        $this->assertAlterSequenceOwnerQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq OWNER TO new_owner'
        );
    }

    public function test_alter_sequence_owner_to_if_exists() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->withIfExists()
            ->ownerTo('new_owner');

        $this->assertAlterSequenceOwnerQuery(
            $builder,
            'ALTER SEQUENCE IF EXISTS user_id_seq OWNER TO new_owner'
        );
    }

    public function test_alter_sequence_rename_to() : void
    {
        $builder = alter()->sequence('old_seq')
            ->renameTo('new_seq');

        $this->assertAlterSequenceRenameQuery(
            $builder,
            'ALTER SEQUENCE old_seq RENAME TO new_seq'
        );
    }

    public function test_alter_sequence_rename_to_if_exists() : void
    {
        $builder = alter()->sequence('old_seq')
            ->withIfExists()
            ->renameTo('new_seq');

        $this->assertAlterSequenceRenameQuery(
            $builder,
            'ALTER SEQUENCE IF EXISTS old_seq RENAME TO new_seq'
        );
    }

    public function test_alter_sequence_rename_to_with_schema() : void
    {
        $builder = alter()->sequence('old_seq', 'public')
            ->renameTo('new_seq');

        $this->assertAlterSequenceRenameQuery(
            $builder,
            'ALTER SEQUENCE public.old_seq RENAME TO new_seq'
        );
    }

    public function test_alter_sequence_restart() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->restart();

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq RESTART'
        );
    }

    public function test_alter_sequence_restart_with_value() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->restartWith(1000);

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq RESTART 1000'
        );
    }

    public function test_alter_sequence_set_logged() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->setLogged();

        $this->assertAlterSequenceLoggingQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq SET LOGGED'
        );
    }

    public function test_alter_sequence_set_logged_if_exists() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->withIfExists()
            ->setLogged();

        $this->assertAlterSequenceLoggingQuery(
            $builder,
            'ALTER SEQUENCE IF EXISTS user_id_seq SET LOGGED'
        );
    }

    public function test_alter_sequence_set_schema() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->setSchema('new_schema');

        $this->assertAlterSequenceSchemaQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq SET SCHEMA new_schema'
        );
    }

    public function test_alter_sequence_set_schema_if_exists() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->withIfExists()
            ->setSchema('new_schema');

        $this->assertAlterSequenceSchemaQuery(
            $builder,
            'ALTER SEQUENCE IF EXISTS user_id_seq SET SCHEMA new_schema'
        );
    }

    public function test_alter_sequence_set_unlogged() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->setUnlogged();

        $this->assertAlterSequenceLoggingQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq SET UNLOGGED'
        );
    }

    public function test_alter_sequence_start_with() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->startWith(100);

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE user_id_seq START 100'
        );
    }

    public function test_alter_sequence_with_schema() : void
    {
        $builder = alter()->sequence('user_id_seq', 'public')
            ->incrementBy(10);

        $this->assertAlterSequenceQuery(
            $builder,
            'ALTER SEQUENCE public.user_id_seq INCREMENT 10'
        );
    }

    public function test_create_sequence_if_not_exists() : void
    {
        $builder = create()->sequence('user_id_seq')->ifNotExists();

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE IF NOT EXISTS user_id_seq'
        );
    }

    public function test_create_sequence_simple() : void
    {
        $builder = create()->sequence('user_id_seq');

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq'
        );
    }

    public function test_create_sequence_with_all_options() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->asType('bigint')
            ->startWith(1)
            ->incrementBy(1)
            ->minValue(1)
            ->maxValue(1000000)
            ->cache(1)
            ->noCycle();

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq AS bigint START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 1000000 CACHE 1 NO CYCLE'
        );
    }

    public function test_create_sequence_with_as_type() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->asType('bigint');

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq AS bigint'
        );
    }

    public function test_create_sequence_with_cache() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->cache(20);

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq CACHE 20'
        );
    }

    public function test_create_sequence_with_cycle() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->cycle();

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq CYCLE'
        );
    }

    public function test_create_sequence_with_increment() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->incrementBy(10);

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq INCREMENT 10'
        );
    }

    public function test_create_sequence_with_max_value() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->maxValue(9999999);

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq MAXVALUE 9999999'
        );
    }

    public function test_create_sequence_with_min_value() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->minValue(1);

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq MINVALUE 1'
        );
    }

    public function test_create_sequence_with_no_cycle() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->noCycle();

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq NO CYCLE'
        );
    }

    public function test_create_sequence_with_no_max_value() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->noMaxValue();

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq NO MAXVALUE'
        );
    }

    public function test_create_sequence_with_no_min_value() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->noMinValue();

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq NO MINVALUE'
        );
    }

    public function test_create_sequence_with_owned_by() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->ownedBy('users', 'id');

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq OWNED BY users.id'
        );
    }

    public function test_create_sequence_with_owned_by_none() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->ownedByNone();

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq OWNED BY "none"'
        );
    }

    public function test_create_sequence_with_owned_by_schema_qualified() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->ownedBy('public.users', 'id');

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq OWNED BY public.users.id'
        );
    }

    public function test_create_sequence_with_schema() : void
    {
        $builder = create()->sequence('user_id_seq', 'public');

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE public.user_id_seq'
        );
    }

    public function test_create_sequence_with_start_value() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->startWith(100);

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE SEQUENCE user_id_seq START 100'
        );
    }

    public function test_create_temporary_sequence() : void
    {
        $builder = create()->sequence('temp_seq')->temporary();

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE TEMPORARY SEQUENCE temp_seq'
        );
    }

    public function test_create_unlogged_sequence() : void
    {
        $builder = create()->sequence('fast_seq')->unlogged();

        $this->assertCreateSequenceQuery(
            $builder,
            'CREATE UNLOGGED SEQUENCE fast_seq'
        );
    }

    public function test_drop_sequence_cascade() : void
    {
        $builder = drop()->sequence('user_id_seq')
            ->cascade();

        $this->assertDropSequenceQuery(
            $builder,
            'DROP SEQUENCE user_id_seq CASCADE'
        );
    }

    public function test_drop_sequence_if_exists() : void
    {
        $builder = drop()->sequence('user_id_seq')->ifExists();

        $this->assertDropSequenceQuery(
            $builder,
            'DROP SEQUENCE IF EXISTS user_id_seq'
        );
    }

    public function test_drop_sequence_if_exists_cascade() : void
    {
        $builder = drop()->sequence('user_id_seq')
            ->ifExists()
            ->cascade();

        $this->assertDropSequenceQuery(
            $builder,
            'DROP SEQUENCE IF EXISTS user_id_seq CASCADE'
        );
    }

    public function test_drop_sequence_multiple() : void
    {
        $builder = drop()->sequence('user_id_seq', 'order_id_seq', 'product_id_seq');

        $this->assertDropSequenceQuery(
            $builder,
            'DROP SEQUENCE user_id_seq, order_id_seq, product_id_seq'
        );
    }

    public function test_drop_sequence_restrict() : void
    {
        $builder = drop()->sequence('user_id_seq')
            ->restrict();

        $this->assertDropSequenceQuery(
            $builder,
            'DROP SEQUENCE user_id_seq'
        );
    }

    public function test_drop_sequence_simple() : void
    {
        $builder = drop()->sequence('user_id_seq');

        $this->assertDropSequenceQuery(
            $builder,
            'DROP SEQUENCE user_id_seq'
        );
    }

    public function test_drop_sequence_with_schema() : void
    {
        $builder = drop()->sequence('public.user_id_seq');

        $this->assertDropSequenceQuery(
            $builder,
            'DROP SEQUENCE public.user_id_seq'
        );
    }
}
