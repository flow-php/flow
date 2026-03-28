<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Function;

use function Flow\PostgreSql\DSL\{alter, call, column_type_integer, column_type_text, create, do_block, drop, func_arg};
use Flow\PostgreSql\QueryBuilder\Schema\Function\ParallelSafety;
use PHPUnit\Framework\TestCase;

final class FunctionBuilderTest extends TestCase
{
    public function test_alter_function_immutable() : void
    {
        $builder = alter()->function('my_func')
            ->arguments(func_arg(column_type_integer()))
            ->immutable();

        self::assertSame('ALTER FUNCTION my_func(int) IMMUTABLE', $builder->toSql());
    }

    public function test_alter_function_parallel_safe() : void
    {
        $builder = alter()->function('my_func')
            ->arguments(func_arg(column_type_integer()))
            ->parallel(ParallelSafety::SAFE);

        self::assertSame('ALTER FUNCTION my_func(int) PARALLEL safe', $builder->toSql());
    }

    public function test_alter_function_rename() : void
    {
        $builder = alter()->function('old_name')
            ->arguments(func_arg(column_type_text()))
            ->renameTo('new_name');

        self::assertSame('ALTER FUNCTION old_name(pg_catalog.text) RENAME TO new_name', $builder->toSql());
    }

    public function test_alter_procedure_rename() : void
    {
        $builder = alter()->procedure('old_proc')
            ->arguments(func_arg(column_type_integer()))
            ->renameTo('new_proc');

        self::assertSame('ALTER PROCEDURE old_proc(int) RENAME TO new_proc', $builder->toSql());
    }

    public function test_call_procedure() : void
    {
        $builder = call('update_stats');

        self::assertSame('CALL update_stats()', $builder->toSql());
    }

    public function test_call_procedure_with_args() : void
    {
        $builder = call('update_stats')->with(123, 'test');

        self::assertSame("CALL update_stats(123, 'test')", $builder->toSql());
    }

    public function test_create_function_plpgsql() : void
    {
        $builder = create()->function('increment')
            ->arguments(func_arg(column_type_integer())->named('val'))
            ->returns(column_type_integer())
            ->language('plpgsql')
            ->as('BEGIN RETURN val + 1; END;');

        self::assertSame(
            'CREATE FUNCTION increment(IN val int) RETURNS int LANGUAGE plpgsql AS $$BEGIN RETURN val + 1; END;$$',
            $builder->toSql()
        );
    }

    public function test_create_function_returns_table() : void
    {
        $builder = create()->function('get_users')
            ->returnsTable(['id' => column_type_integer(), 'name' => column_type_text()])
            ->language('sql')
            ->as('SELECT id, name FROM users');

        self::assertSame(
            'CREATE FUNCTION get_users() RETURNS TABLE (id int, name pg_catalog.text) LANGUAGE sql AS $$SELECT id, name FROM users$$',
            $builder->toSql()
        );
    }

    public function test_create_function_simple() : void
    {
        $builder = create()->function('add_numbers')
            ->arguments(func_arg(column_type_integer())->named('a'), func_arg(column_type_integer())->named('b'))
            ->returns(column_type_integer())
            ->language('sql')
            ->as('SELECT a + b');

        self::assertSame(
            'CREATE FUNCTION add_numbers(IN a int, IN b int) RETURNS int LANGUAGE sql AS $$SELECT a + b$$',
            $builder->toSql()
        );
    }

    public function test_create_function_with_options() : void
    {
        $builder = create()->function('compute')
            ->arguments(func_arg(column_type_integer())->named('x'))
            ->returns(column_type_integer())
            ->language('sql')
            ->immutable()
            ->parallel(ParallelSafety::SAFE)
            ->strict()
            ->as('SELECT x * 2');

        self::assertSame(
            'CREATE FUNCTION compute(IN x int) RETURNS int LANGUAGE sql IMMUTABLE PARALLEL safe RETURNS NULL ON NULL INPUT AS $$SELECT x * 2$$',
            $builder->toSql()
        );
    }

    public function test_create_function_with_or_replace() : void
    {
        $builder = create()->function('my_func')
            ->orReplace()
            ->returns(column_type_integer())
            ->language('sql')
            ->as('SELECT 1');

        self::assertSame(
            'CREATE OR REPLACE FUNCTION my_func() RETURNS int LANGUAGE sql AS $$SELECT 1$$',
            $builder->toSql()
        );
    }

    public function test_create_procedure() : void
    {
        $builder = create()->procedure('update_stats')
            ->arguments(func_arg(column_type_integer())->named('user_id'))
            ->language('plpgsql')
            ->as('BEGIN UPDATE stats SET count = count + 1 WHERE id = user_id; END;');

        self::assertSame(
            'CREATE PROCEDURE update_stats(IN user_id int) LANGUAGE plpgsql AS $$BEGIN UPDATE stats SET count = count + 1 WHERE id = user_id; END;$$',
            $builder->toSql()
        );
    }

    public function test_create_procedure_with_or_replace() : void
    {
        $builder = create()->procedure('my_proc')
            ->orReplace()
            ->language('sql')
            ->as('SELECT 1');

        self::assertSame(
            'CREATE OR REPLACE PROCEDURE my_proc() LANGUAGE sql AS $$SELECT 1$$',
            $builder->toSql()
        );
    }

    public function test_do_block() : void
    {
        $builder = do_block('BEGIN RAISE NOTICE $$Hello$$; END;');

        self::assertSame('DO $outer$BEGIN RAISE NOTICE $$Hello$$; END;$outer$ LANGUAGE plpgsql', $builder->toSql());
    }

    public function test_do_block_with_language() : void
    {
        $builder = do_block('SELECT 1')->language('sql');

        self::assertSame('DO $$SELECT 1$$ LANGUAGE sql', $builder->toSql());
    }

    public function test_drop_function() : void
    {
        $builder = drop()->function('my_func');

        self::assertSame('DROP FUNCTION my_func', $builder->toSql());
    }

    public function test_drop_function_if_exists_cascade() : void
    {
        $builder = drop()->function('my_func')
            ->ifExists()
            ->arguments(func_arg(column_type_integer()), func_arg(column_type_text()))
            ->cascade();

        self::assertSame('DROP FUNCTION IF EXISTS my_func(int, pg_catalog.text) CASCADE', $builder->toSql());
    }

    public function test_drop_procedure() : void
    {
        $builder = drop()->procedure('my_proc');

        self::assertSame('DROP PROCEDURE my_proc', $builder->toSql());
    }

    public function test_drop_procedure_if_exists() : void
    {
        $builder = drop()->procedure('my_proc')
            ->ifExists()
            ->arguments(func_arg(column_type_integer()))
            ->cascade();

        self::assertSame('DROP PROCEDURE IF EXISTS my_proc(int) CASCADE', $builder->toSql());
    }
}
