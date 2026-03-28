<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\CreateSequence;

use function Flow\PostgreSql\DSL\{
    alter,
    create,
    drop
};

use PHPUnit\Framework\TestCase;

final class SequenceBuilderTest extends TestCase
{
    public function test_alter_sequence_as_type() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->asType('smallint');

        self::assertSame(
            'ALTER SEQUENCE user_id_seq AS smallint',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_cache() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->cache(20);

        self::assertSame(
            'ALTER SEQUENCE user_id_seq CACHE 20',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_cycle() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->cycle();

        self::assertSame(
            'ALTER SEQUENCE user_id_seq CYCLE',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_if_exists() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->withIfExists()
            ->incrementBy(10);

        self::assertSame(
            'ALTER SEQUENCE IF EXISTS user_id_seq INCREMENT 10',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_increment_by() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->incrementBy(10);

        self::assertSame(
            'ALTER SEQUENCE user_id_seq INCREMENT 10',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_max_value() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->maxValue(9999999);

        self::assertSame(
            'ALTER SEQUENCE user_id_seq MAXVALUE 9999999',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_min_value() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->minValue(1);

        self::assertSame(
            'ALTER SEQUENCE user_id_seq MINVALUE 1',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_multiple_options() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->incrementBy(10)
            ->minValue(1)
            ->maxValue(1000000)
            ->cache(5);

        self::assertSame(
            'ALTER SEQUENCE user_id_seq INCREMENT 10 MINVALUE 1 MAXVALUE 1000000 CACHE 5',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_no_cycle() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->noCycle();

        self::assertSame(
            'ALTER SEQUENCE user_id_seq NO CYCLE',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_no_max_value() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->noMaxValue();

        self::assertSame(
            'ALTER SEQUENCE user_id_seq NO MAXVALUE',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_no_min_value() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->noMinValue();

        self::assertSame(
            'ALTER SEQUENCE user_id_seq NO MINVALUE',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_owned_by() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->ownedBy('users', 'id');

        self::assertSame(
            'ALTER SEQUENCE user_id_seq OWNED BY users.id',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_owned_by_none() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->ownedByNone();

        self::assertSame(
            'ALTER SEQUENCE user_id_seq OWNED BY "none"',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_owner_to() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->ownerTo('new_owner');

        self::assertSame(
            'ALTER SEQUENCE user_id_seq OWNER TO new_owner',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_owner_to_if_exists() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->withIfExists()
            ->ownerTo('new_owner');

        self::assertSame(
            'ALTER SEQUENCE IF EXISTS user_id_seq OWNER TO new_owner',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_rename_to() : void
    {
        $builder = alter()->sequence('old_seq')
            ->renameTo('new_seq');

        self::assertSame(
            'ALTER SEQUENCE old_seq RENAME TO new_seq',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_rename_to_if_exists() : void
    {
        $builder = alter()->sequence('old_seq')
            ->withIfExists()
            ->renameTo('new_seq');

        self::assertSame(
            'ALTER SEQUENCE IF EXISTS old_seq RENAME TO new_seq',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_rename_to_with_schema() : void
    {
        $builder = alter()->sequence('old_seq', 'public')
            ->renameTo('new_seq');

        self::assertSame(
            'ALTER SEQUENCE public.old_seq RENAME TO new_seq',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_restart() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->restart();

        self::assertSame(
            'ALTER SEQUENCE user_id_seq RESTART',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_restart_with_value() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->restartWith(1000);

        self::assertSame(
            'ALTER SEQUENCE user_id_seq RESTART 1000',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_set_logged() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->setLogged();

        self::assertSame(
            'ALTER SEQUENCE user_id_seq SET LOGGED',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_set_logged_if_exists() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->withIfExists()
            ->setLogged();

        self::assertSame(
            'ALTER SEQUENCE IF EXISTS user_id_seq SET LOGGED',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_set_schema() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->setSchema('new_schema');

        self::assertSame(
            'ALTER SEQUENCE user_id_seq SET SCHEMA new_schema',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_set_schema_if_exists() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->withIfExists()
            ->setSchema('new_schema');

        self::assertSame(
            'ALTER SEQUENCE IF EXISTS user_id_seq SET SCHEMA new_schema',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_set_unlogged() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->setUnlogged();

        self::assertSame(
            'ALTER SEQUENCE user_id_seq SET UNLOGGED',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_start_with() : void
    {
        $builder = alter()->sequence('user_id_seq')
            ->startWith(100);

        self::assertSame(
            'ALTER SEQUENCE user_id_seq START 100',
            $builder->toSql()
        );
    }

    public function test_alter_sequence_with_schema() : void
    {
        $builder = alter()->sequence('user_id_seq', 'public')
            ->incrementBy(10);

        self::assertSame(
            'ALTER SEQUENCE public.user_id_seq INCREMENT 10',
            $builder->toSql()
        );
    }

    public function test_create_sequence_if_not_exists() : void
    {
        $builder = create()->sequence('user_id_seq')->ifNotExists();

        self::assertSame(
            'CREATE SEQUENCE IF NOT EXISTS user_id_seq',
            $builder->toSql()
        );
    }

    public function test_create_sequence_simple() : void
    {
        $builder = create()->sequence('user_id_seq');

        self::assertSame(
            'CREATE SEQUENCE user_id_seq',
            $builder->toSql()
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

        self::assertSame(
            'CREATE SEQUENCE user_id_seq AS bigint START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 1000000 CACHE 1 NO CYCLE',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_as_type() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->asType('bigint');

        self::assertSame(
            'CREATE SEQUENCE user_id_seq AS bigint',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_cache() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->cache(20);

        self::assertSame(
            'CREATE SEQUENCE user_id_seq CACHE 20',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_cycle() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->cycle();

        self::assertSame(
            'CREATE SEQUENCE user_id_seq CYCLE',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_increment() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->incrementBy(10);

        self::assertSame(
            'CREATE SEQUENCE user_id_seq INCREMENT 10',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_max_value() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->maxValue(9999999);

        self::assertSame(
            'CREATE SEQUENCE user_id_seq MAXVALUE 9999999',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_min_value() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->minValue(1);

        self::assertSame(
            'CREATE SEQUENCE user_id_seq MINVALUE 1',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_no_cycle() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->noCycle();

        self::assertSame(
            'CREATE SEQUENCE user_id_seq NO CYCLE',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_no_max_value() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->noMaxValue();

        self::assertSame(
            'CREATE SEQUENCE user_id_seq NO MAXVALUE',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_no_min_value() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->noMinValue();

        self::assertSame(
            'CREATE SEQUENCE user_id_seq NO MINVALUE',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_owned_by() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->ownedBy('users', 'id');

        self::assertSame(
            'CREATE SEQUENCE user_id_seq OWNED BY users.id',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_owned_by_none() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->ownedByNone();

        self::assertSame(
            'CREATE SEQUENCE user_id_seq OWNED BY "none"',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_owned_by_schema_qualified() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->ownedBy('public.users', 'id');

        self::assertSame(
            'CREATE SEQUENCE user_id_seq OWNED BY public.users.id',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_schema() : void
    {
        $builder = create()->sequence('user_id_seq', 'public');

        self::assertSame(
            'CREATE SEQUENCE public.user_id_seq',
            $builder->toSql()
        );
    }

    public function test_create_sequence_with_start_value() : void
    {
        $builder = create()->sequence('user_id_seq')
            ->startWith(100);

        self::assertSame(
            'CREATE SEQUENCE user_id_seq START 100',
            $builder->toSql()
        );
    }

    public function test_create_temporary_sequence() : void
    {
        $builder = create()->sequence('temp_seq')->temporary();

        self::assertSame(
            'CREATE TEMPORARY SEQUENCE temp_seq',
            $builder->toSql()
        );
    }

    public function test_create_unlogged_sequence() : void
    {
        $builder = create()->sequence('fast_seq')->unlogged();

        self::assertSame(
            'CREATE UNLOGGED SEQUENCE fast_seq',
            $builder->toSql()
        );
    }

    public function test_drop_sequence_cascade() : void
    {
        $builder = drop()->sequence('user_id_seq')
            ->cascade();

        self::assertSame(
            'DROP SEQUENCE user_id_seq CASCADE',
            $builder->toSql()
        );
    }

    public function test_drop_sequence_if_exists() : void
    {
        $builder = drop()->sequence('user_id_seq')->ifExists();

        self::assertSame(
            'DROP SEQUENCE IF EXISTS user_id_seq',
            $builder->toSql()
        );
    }

    public function test_drop_sequence_if_exists_cascade() : void
    {
        $builder = drop()->sequence('user_id_seq')
            ->ifExists()
            ->cascade();

        self::assertSame(
            'DROP SEQUENCE IF EXISTS user_id_seq CASCADE',
            $builder->toSql()
        );
    }

    public function test_drop_sequence_multiple() : void
    {
        $builder = drop()->sequence('user_id_seq', 'order_id_seq', 'product_id_seq');

        self::assertSame(
            'DROP SEQUENCE user_id_seq, order_id_seq, product_id_seq',
            $builder->toSql()
        );
    }

    public function test_drop_sequence_restrict() : void
    {
        $builder = drop()->sequence('user_id_seq')
            ->restrict();

        self::assertSame(
            'DROP SEQUENCE user_id_seq',
            $builder->toSql()
        );
    }

    public function test_drop_sequence_simple() : void
    {
        $builder = drop()->sequence('user_id_seq');

        self::assertSame(
            'DROP SEQUENCE user_id_seq',
            $builder->toSql()
        );
    }

    public function test_drop_sequence_with_schema() : void
    {
        $builder = drop()->sequence('public.user_id_seq');

        self::assertSame(
            'DROP SEQUENCE public.user_id_seq',
            $builder->toSql()
        );
    }
}
