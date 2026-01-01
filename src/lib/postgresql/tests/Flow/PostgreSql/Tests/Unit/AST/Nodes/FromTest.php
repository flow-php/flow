<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Nodes;

use function Flow\PostgreSql\DSL\{col, derived, eq, func, literal, select, sql_parse, star, table, table_func};

use Flow\PostgreSql\AST\Nodes\Exception\InvalidFromNodeException;
use Flow\PostgreSql\AST\Nodes\{From, Tables};
use Flow\PostgreSql\AST\Nodes\Statement\SelectStatement;
use Flow\PostgreSql\Protobuf\AST\Node;
use PHPUnit\Framework\TestCase;

final class FromTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_accepts_join_expr_node() : void
    {
        $statement = sql_parse(
            select(star())
                ->from(table('users'))
                ->join(table('orders'), eq(col('id', 'users'), col('user_id', 'orders')))
                ->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        self::assertFalse($from->isEmpty());
    }

    public function test_accepts_range_function_node() : void
    {
        $statement = sql_parse(
            select(star())
                ->from(table_func(func('generate_series', [literal(1), literal(10)])))
                ->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        self::assertFalse($from->isEmpty());
    }

    public function test_accepts_range_subselect_node() : void
    {
        $statement = sql_parse(
            select(star())
                ->from(derived(select(literal(1)), 't'))
                ->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        self::assertFalse($from->isEmpty());
    }

    public function test_accepts_range_var_node() : void
    {
        $statement = sql_parse(
            select(star())->from(table('users'))->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        self::assertFalse($from->isEmpty());
    }

    public function test_count_returns_number_of_from_nodes() : void
    {
        $statement = sql_parse(
            select(star())->from(table('users'), table('orders'))->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertCount(2, $statement->from());
    }

    public function test_count_returns_zero_for_empty_from() : void
    {
        $from = new From([]);
        self::assertCount(0, $from);
    }

    public function test_empty_nodes_array_creates_empty_from() : void
    {
        $from = new From([]);
        self::assertTrue($from->isEmpty());
    }

    public function test_has_function_returns_false_for_regular_table() : void
    {
        $statement = sql_parse(
            select(star())->from(table('users'))->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertFalse($statement->from()->hasFunction());
    }

    public function test_has_function_returns_true_for_function_in_from() : void
    {
        $statement = sql_parse(
            select(star())
                ->from(table_func(func('generate_series', [literal(1), literal(10)])))
                ->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->from()->hasFunction());
    }

    public function test_has_function_returns_true_for_unnest_function() : void
    {
        $statement = sql_parse('SELECT * FROM unnest(ARRAY[1,2,3])')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->from()->hasFunction());
    }

    public function test_tables_returns_empty_collection_for_empty_from() : void
    {
        $from = new From([]);

        $tables = $from->tables();

        self::assertInstanceOf(Tables::class, $tables);
        self::assertTrue($tables->isEmpty());
    }

    public function test_tables_returns_empty_collection_for_function() : void
    {
        $statement = sql_parse(
            select(star())
                ->from(table_func(func('generate_series', [literal(1), literal(10)])))
                ->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $tables = $statement->from()->tables();

        self::assertTrue($tables->isEmpty());
    }

    public function test_tables_returns_empty_collection_for_join() : void
    {
        $statement = sql_parse(
            select(star())
                ->from(table('users'))
                ->join(table('orders'), eq(col('id', 'users'), col('user_id', 'orders')))
                ->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $tables = $statement->from()->tables();

        self::assertTrue($tables->isEmpty());
    }

    public function test_tables_returns_empty_collection_for_subquery() : void
    {
        $statement = sql_parse(
            select(star())
                ->from(derived(select(literal(1)), 't'))
                ->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $tables = $statement->from()->tables();

        self::assertTrue($tables->isEmpty());
    }

    public function test_tables_returns_multiple_tables() : void
    {
        $statement = sql_parse(
            select(star())->from(table('users'), table('orders'))->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $tables = $statement->from()->tables();

        self::assertCount(2, $tables);
        self::assertSame('users', $tables->first()?->name());
        self::assertSame('orders', $tables->last()?->name());
    }

    public function test_tables_returns_single_table() : void
    {
        $statement = sql_parse(
            select(star())->from(table('users'))->toSql()
        )->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $tables = $statement->from()->tables();

        self::assertTrue($tables->isSingle());
        self::assertSame('users', $tables->first()?->name());
    }

    public function test_throws_exception_for_invalid_node() : void
    {
        $invalidNode = new Node();

        $this->expectException(InvalidFromNodeException::class);
        $this->expectExceptionMessage('Invalid FROM clause node type');

        new From([$invalidNode]);
    }
}
