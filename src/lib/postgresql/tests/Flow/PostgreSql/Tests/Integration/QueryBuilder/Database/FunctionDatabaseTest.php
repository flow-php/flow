<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder\Database;

use function Flow\PostgreSql\DSL\{
    agg_count,
    alter,
    col,
    column_type_integer,
    create,
    drop,
    eq,
    func,
    func_arg,
    literal,
    select,
    table
};
use Flow\PostgreSql\QueryBuilder\Schema\Function\ParallelSafety;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

final class FunctionDatabaseTest extends PostgreSqlTestCase
{
    private const FUNCTION_NAME = 'flow_postgres_test_func';

    private const FUNCTION_NAME_2 = 'flow_postgres_test_func_2';

    private const FUNCTION_NAME_RENAMED = 'flow_postgres_test_func_renamed';

    protected function tearDown() : void
    {
        $this->pgsqlContext()->dropFunctionIfExists(self::FUNCTION_NAME);
        $this->pgsqlContext()->dropFunctionIfExists(self::FUNCTION_NAME_2);
        $this->pgsqlContext()->dropFunctionIfExists(self::FUNCTION_NAME_RENAMED);

        parent::tearDown();
    }

    public function test_alter_function_rename() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(column_type_integer())
                ->language('sql')
                ->as('SELECT 42')
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            alter()->function(self::FUNCTION_NAME)
                ->arguments()
                ->renameTo(self::FUNCTION_NAME_RENAMED)
                ->toSql()
        );

        self::assertFalse($this->functionExists(self::FUNCTION_NAME));
        self::assertTrue($this->functionExists(self::FUNCTION_NAME_RENAMED));
    }

    public function test_alter_function_set_volatility() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(column_type_integer())
                ->language('sql')
                ->as('SELECT 42')
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            alter()->function(self::FUNCTION_NAME)
                ->arguments()
                ->immutable()
                ->toSql()
        );

        self::assertTrue($this->functionExists(self::FUNCTION_NAME));
    }

    public function test_create_function_returns_void() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returnsVoid()
                ->language('sql')
                ->as('SELECT 1')
                ->toSql()
        );

        self::assertTrue($this->functionExists(self::FUNCTION_NAME));
    }

    public function test_create_function_with_arguments() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments(
                    func_arg(column_type_integer())->named('a'),
                    func_arg(column_type_integer())->named('b')
                )
                ->returns(column_type_integer())
                ->language('sql')
                ->as('SELECT a + b')
                ->toSql()
        );

        self::assertTrue($this->functionExists(self::FUNCTION_NAME));

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(func(self::FUNCTION_NAME, [literal(10), literal(5)])->as('result'))->toSql()
        );
        self::assertSame(15, $row['result']);
    }

    public function test_create_function_with_default_argument() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments(
                    func_arg(column_type_integer())->named('a'),
                    func_arg(column_type_integer())->named('b')->default('10')
                )
                ->returns(column_type_integer())
                ->language('sql')
                ->as('SELECT a + b')
                ->toSql()
        );

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(func(self::FUNCTION_NAME, [literal(5)])->as('result'))->toSql()
        );
        self::assertSame(15, $row['result']);

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(func(self::FUNCTION_NAME, [literal(5), literal(3)])->as('result'))->toSql()
        );
        self::assertSame(8, $row['result']);
    }

    public function test_create_function_with_parallel_safety() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(column_type_integer())
                ->language('sql')
                ->parallel(ParallelSafety::SAFE)
                ->as('SELECT 42')
                ->toSql()
        );

        self::assertTrue($this->functionExists(self::FUNCTION_NAME));
    }

    public function test_create_function_with_volatility() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(column_type_integer())
                ->language('sql')
                ->immutable()
                ->as('SELECT 42')
                ->toSql()
        );

        self::assertTrue($this->functionExists(self::FUNCTION_NAME));
    }

    public function test_create_or_replace_function() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(column_type_integer())
                ->language('sql')
                ->as('SELECT 1')
                ->toSql()
        );

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(func(self::FUNCTION_NAME)->as('result'))->toSql()
        );
        self::assertSame(1, $row['result']);

        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->orReplace()
                ->arguments()
                ->returns(column_type_integer())
                ->language('sql')
                ->as('SELECT 2')
                ->toSql()
        );

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(func(self::FUNCTION_NAME)->as('result'))->toSql()
        );
        self::assertSame(2, $row['result']);
    }

    public function test_create_simple_function() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(column_type_integer())
                ->language('sql')
                ->as('SELECT 42')
                ->toSql()
        );

        self::assertTrue($this->functionExists(self::FUNCTION_NAME));

        $row = $this->pgsqlContext()->client()->fetchOne(
            select(func(self::FUNCTION_NAME)->as('result'))->toSql()
        );
        self::assertSame(42, $row['result']);
    }

    public function test_drop_function() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments()
                ->returns(column_type_integer())
                ->language('sql')
                ->as('SELECT 42')
                ->toSql()
        );

        self::assertTrue($this->functionExists(self::FUNCTION_NAME));

        $this->pgsqlContext()->client()->execute(
            drop()->function(self::FUNCTION_NAME)->toSql()
        );

        self::assertFalse($this->functionExists(self::FUNCTION_NAME));
    }

    public function test_drop_function_if_exists() : void
    {
        $this->expectNotToPerformAssertions();

        $this->pgsqlContext()->client()->execute(
            drop()->function(self::FUNCTION_NAME)->ifExists()->toSql()
        );
    }

    public function test_drop_function_with_arguments() : void
    {
        $this->pgsqlContext()->client()->execute(
            create()->function(self::FUNCTION_NAME)
                ->arguments(func_arg(column_type_integer())->named('a'))
                ->returns(column_type_integer())
                ->language('sql')
                ->as('SELECT a * 2')
                ->toSql()
        );

        $this->pgsqlContext()->client()->execute(
            drop()->function(self::FUNCTION_NAME)
                ->arguments(func_arg(column_type_integer()))
                ->toSql()
        );

        self::assertFalse($this->functionExists(self::FUNCTION_NAME));
    }

    protected function functionExists(string $name) : bool
    {
        $row = $this->pgsqlContext()->client()->fetchOne(
            select(agg_count()->as('cnt'))
                ->from(table('pg_proc'))
                ->where(eq(col('proname'), literal($name)))
                ->toSql()
        );

        return $row['cnt'] > 0;
    }
}
