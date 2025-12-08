<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{alter_function, alter_procedure, call, create_function, create_procedure, do_block, drop_function, drop_procedure, func_arg};
use Flow\PgQuery\QueryBuilder\Schema\Function\ParallelSafety;
use Flow\PgQuery\Tests\Integration\QueryBuilder\Assertions\QueryBuilderAssertions;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class FunctionBuilderTest extends TestCase
{
    use QueryBuilderAssertions;

    #[Test]
    public function test_alter_function_immutable() : void
    {
        $builder = alter_function('my_func')
            ->arguments(func_arg('integer'))
            ->immutable();

        $this->assertAlterFunctionQuery($builder, 'ALTER FUNCTION my_func("integer") IMMUTABLE');
    }

    #[Test]
    public function test_alter_function_parallel_safe() : void
    {
        $builder = alter_function('my_func')
            ->arguments(func_arg('integer'))
            ->parallel(ParallelSafety::SAFE);

        $this->assertAlterFunctionQuery($builder, 'ALTER FUNCTION my_func("integer") PARALLEL safe');
    }

    #[Test]
    public function test_alter_function_rename() : void
    {
        $builder = alter_function('old_name')
            ->arguments(func_arg('text'))
            ->renameTo('new_name');

        $this->assertAlterFunctionRenameQuery($builder, 'ALTER FUNCTION old_name(text) RENAME TO new_name');
    }

    #[Test]
    public function test_alter_procedure_rename() : void
    {
        $builder = alter_procedure('old_proc')
            ->arguments(func_arg('integer'))
            ->renameTo('new_proc');

        $this->assertAlterProcedureRenameQuery($builder, 'ALTER PROCEDURE old_proc("integer") RENAME TO new_proc');
    }

    #[Test]
    public function test_call_procedure() : void
    {
        $builder = call('update_stats');

        $this->assertCallQuery($builder, 'CALL update_stats()');
    }

    #[Test]
    public function test_call_procedure_with_args() : void
    {
        $builder = call('update_stats')->with(123, 'test');

        $this->assertCallQuery($builder, "CALL update_stats(123, 'test')");
    }

    #[Test]
    public function test_create_function_plpgsql() : void
    {
        $builder = create_function('increment')
            ->arguments(func_arg('integer')->named('val'))
            ->returns('integer')
            ->language('plpgsql')
            ->as('BEGIN RETURN val + 1; END;');

        $this->assertCreateFunctionQuery(
            $builder,
            'CREATE FUNCTION increment(IN val "integer") RETURNS "integer" LANGUAGE plpgsql AS $$BEGIN RETURN val + 1; END;$$'
        );
    }

    #[Test]
    public function test_create_function_returns_table() : void
    {
        $builder = create_function('get_users')
            ->returnsTable(['id' => 'integer', 'name' => 'text'])
            ->language('sql')
            ->as('SELECT id, name FROM users');

        $this->assertCreateFunctionQuery(
            $builder,
            'CREATE FUNCTION get_users() RETURNS TABLE (id "integer", name text) LANGUAGE sql AS $$SELECT id, name FROM users$$'
        );
    }

    #[Test]
    public function test_create_function_simple() : void
    {
        $builder = create_function('add_numbers')
            ->arguments(func_arg('integer')->named('a'), func_arg('integer')->named('b'))
            ->returns('integer')
            ->language('sql')
            ->as('SELECT a + b');

        $this->assertCreateFunctionQuery(
            $builder,
            'CREATE FUNCTION add_numbers(IN a "integer", IN b "integer") RETURNS "integer" LANGUAGE sql AS $$SELECT a + b$$'
        );
    }

    #[Test]
    public function test_create_function_with_options() : void
    {
        $builder = create_function('compute')
            ->arguments(func_arg('integer')->named('x'))
            ->returns('integer')
            ->language('sql')
            ->immutable()
            ->parallel(ParallelSafety::SAFE)
            ->strict()
            ->as('SELECT x * 2');

        $this->assertCreateFunctionQuery(
            $builder,
            'CREATE FUNCTION compute(IN x "integer") RETURNS "integer" LANGUAGE sql IMMUTABLE PARALLEL safe RETURNS NULL ON NULL INPUT AS $$SELECT x * 2$$'
        );
    }

    #[Test]
    public function test_create_function_with_or_replace() : void
    {
        $builder = create_function('my_func')
            ->orReplace()
            ->returns('integer')
            ->language('sql')
            ->as('SELECT 1');

        $this->assertCreateFunctionQuery(
            $builder,
            'CREATE OR REPLACE FUNCTION my_func() RETURNS "integer" LANGUAGE sql AS $$SELECT 1$$'
        );
    }

    #[Test]
    public function test_create_procedure() : void
    {
        $builder = create_procedure('update_stats')
            ->arguments(func_arg('integer')->named('user_id'))
            ->language('plpgsql')
            ->as('BEGIN UPDATE stats SET count = count + 1 WHERE id = user_id; END;');

        $this->assertCreateProcedureQuery(
            $builder,
            'CREATE PROCEDURE update_stats(IN user_id "integer") LANGUAGE plpgsql AS $$BEGIN UPDATE stats SET count = count + 1 WHERE id = user_id; END;$$'
        );
    }

    #[Test]
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

    #[Test]
    public function test_do_block() : void
    {
        $builder = do_block('BEGIN RAISE NOTICE $$Hello$$; END;');

        $this->assertDoQuery($builder, 'DO $outer$BEGIN RAISE NOTICE $$Hello$$; END;$outer$ LANGUAGE plpgsql');
    }

    #[Test]
    public function test_do_block_with_language() : void
    {
        $builder = do_block('SELECT 1')->language('sql');

        $this->assertDoQuery($builder, 'DO $$SELECT 1$$ LANGUAGE sql');
    }

    #[Test]
    public function test_drop_function() : void
    {
        $builder = drop_function('my_func');

        $this->assertDropFunctionQuery($builder, 'DROP FUNCTION my_func');
    }

    #[Test]
    public function test_drop_function_if_exists_cascade() : void
    {
        $builder = drop_function('my_func')
            ->ifExists()
            ->arguments(func_arg('integer'), func_arg('text'))
            ->cascade();

        $this->assertDropFunctionQuery($builder, 'DROP FUNCTION IF EXISTS my_func("integer", text) CASCADE');
    }

    #[Test]
    public function test_drop_procedure() : void
    {
        $builder = drop_procedure('my_proc');

        $this->assertDropProcedureQuery($builder, 'DROP PROCEDURE my_proc');
    }

    #[Test]
    public function test_drop_procedure_if_exists() : void
    {
        $builder = drop_procedure('my_proc')
            ->ifExists()
            ->arguments(func_arg('integer'))
            ->cascade();

        $this->assertDropProcedureQuery($builder, 'DROP PROCEDURE IF EXISTS my_proc("integer") CASCADE');
    }
}
