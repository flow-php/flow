<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{alter_function, alter_procedure, call, create_function, create_procedure, do_block, drop_function, drop_procedure, func_arg, sql_type_integer, sql_type_text};
use Flow\PgQuery\QueryBuilder\Schema\Function\ParallelSafety;

final class FunctionBuilderTest extends PGQueryTestCase
{
    public function test_alter_function_immutable() : void
    {
        $builder = alter_function('my_func')
            ->arguments(func_arg(sql_type_integer()))
            ->immutable();

        $this->assertAlterFunctionQuery($builder, 'ALTER FUNCTION my_func(int) IMMUTABLE');
    }

    public function test_alter_function_parallel_safe() : void
    {
        $builder = alter_function('my_func')
            ->arguments(func_arg(sql_type_integer()))
            ->parallel(ParallelSafety::SAFE);

        $this->assertAlterFunctionQuery($builder, 'ALTER FUNCTION my_func(int) PARALLEL safe');
    }

    public function test_alter_function_rename() : void
    {
        $builder = alter_function('old_name')
            ->arguments(func_arg(sql_type_text()))
            ->renameTo('new_name');

        $this->assertAlterFunctionRenameQuery($builder, 'ALTER FUNCTION old_name(pg_catalog.text) RENAME TO new_name');
    }

    public function test_alter_procedure_rename() : void
    {
        $builder = alter_procedure('old_proc')
            ->arguments(func_arg(sql_type_integer()))
            ->renameTo('new_proc');

        $this->assertAlterProcedureRenameQuery($builder, 'ALTER PROCEDURE old_proc(int) RENAME TO new_proc');
    }

    public function test_call_procedure() : void
    {
        $builder = call('update_stats');

        $this->assertCallQuery($builder, 'CALL update_stats()');
    }

    public function test_call_procedure_with_args() : void
    {
        $builder = call('update_stats')->with(123, 'test');

        $this->assertCallQuery($builder, "CALL update_stats(123, 'test')");
    }

    public function test_create_function_plpgsql() : void
    {
        $builder = create_function('increment')
            ->arguments(func_arg(sql_type_integer())->named('val'))
            ->returns(sql_type_integer())
            ->language('plpgsql')
            ->as('BEGIN RETURN val + 1; END;');

        $this->assertCreateFunctionQuery(
            $builder,
            'CREATE FUNCTION increment(IN val int) RETURNS int LANGUAGE plpgsql AS $$BEGIN RETURN val + 1; END;$$'
        );
    }

    public function test_create_function_returns_table() : void
    {
        $builder = create_function('get_users')
            ->returnsTable(['id' => sql_type_integer(), 'name' => sql_type_text()])
            ->language('sql')
            ->as('SELECT id, name FROM users');

        $this->assertCreateFunctionQuery(
            $builder,
            'CREATE FUNCTION get_users() RETURNS TABLE (id int, name pg_catalog.text) LANGUAGE sql AS $$SELECT id, name FROM users$$'
        );
    }

    public function test_create_function_simple() : void
    {
        $builder = create_function('add_numbers')
            ->arguments(func_arg(sql_type_integer())->named('a'), func_arg(sql_type_integer())->named('b'))
            ->returns(sql_type_integer())
            ->language('sql')
            ->as('SELECT a + b');

        $this->assertCreateFunctionQuery(
            $builder,
            'CREATE FUNCTION add_numbers(IN a int, IN b int) RETURNS int LANGUAGE sql AS $$SELECT a + b$$'
        );
    }

    public function test_create_function_with_options() : void
    {
        $builder = create_function('compute')
            ->arguments(func_arg(sql_type_integer())->named('x'))
            ->returns(sql_type_integer())
            ->language('sql')
            ->immutable()
            ->parallel(ParallelSafety::SAFE)
            ->strict()
            ->as('SELECT x * 2');

        $this->assertCreateFunctionQuery(
            $builder,
            'CREATE FUNCTION compute(IN x int) RETURNS int LANGUAGE sql IMMUTABLE PARALLEL safe RETURNS NULL ON NULL INPUT AS $$SELECT x * 2$$'
        );
    }

    public function test_create_function_with_or_replace() : void
    {
        $builder = create_function('my_func')
            ->orReplace()
            ->returns(sql_type_integer())
            ->language('sql')
            ->as('SELECT 1');

        $this->assertCreateFunctionQuery(
            $builder,
            'CREATE OR REPLACE FUNCTION my_func() RETURNS int LANGUAGE sql AS $$SELECT 1$$'
        );
    }

    public function test_create_procedure() : void
    {
        $builder = create_procedure('update_stats')
            ->arguments(func_arg(sql_type_integer())->named('user_id'))
            ->language('plpgsql')
            ->as('BEGIN UPDATE stats SET count = count + 1 WHERE id = user_id; END;');

        $this->assertCreateProcedureQuery(
            $builder,
            'CREATE PROCEDURE update_stats(IN user_id int) LANGUAGE plpgsql AS $$BEGIN UPDATE stats SET count = count + 1 WHERE id = user_id; END;$$'
        );
    }

    public function test_create_procedure_with_or_replace() : void
    {
        $builder = create_procedure('my_proc')
            ->orReplace()
            ->language('sql')
            ->as('SELECT 1');

        $this->assertCreateProcedureQuery(
            $builder,
            'CREATE OR REPLACE PROCEDURE my_proc() LANGUAGE sql AS $$SELECT 1$$'
        );
    }

    public function test_do_block() : void
    {
        $builder = do_block('BEGIN RAISE NOTICE $$Hello$$; END;');

        $this->assertDoQuery($builder, 'DO $outer$BEGIN RAISE NOTICE $$Hello$$; END;$outer$ LANGUAGE plpgsql');
    }

    public function test_do_block_with_language() : void
    {
        $builder = do_block('SELECT 1')->language('sql');

        $this->assertDoQuery($builder, 'DO $$SELECT 1$$ LANGUAGE sql');
    }

    public function test_drop_function() : void
    {
        $builder = drop_function('my_func');

        $this->assertDropFunctionQuery($builder, 'DROP FUNCTION my_func');
    }

    public function test_drop_function_if_exists_cascade() : void
    {
        $builder = drop_function('my_func')
            ->ifExists()
            ->arguments(func_arg(sql_type_integer()), func_arg(sql_type_text()))
            ->cascade();

        $this->assertDropFunctionQuery($builder, 'DROP FUNCTION IF EXISTS my_func(int, pg_catalog.text) CASCADE');
    }

    public function test_drop_procedure() : void
    {
        $builder = drop_procedure('my_proc');

        $this->assertDropProcedureQuery($builder, 'DROP PROCEDURE my_proc');
    }

    public function test_drop_procedure_if_exists() : void
    {
        $builder = drop_procedure('my_proc')
            ->ifExists()
            ->arguments(func_arg(sql_type_integer()))
            ->cascade();

        $this->assertDropProcedureQuery($builder, 'DROP PROCEDURE IF EXISTS my_proc(int) CASCADE');
    }
}
