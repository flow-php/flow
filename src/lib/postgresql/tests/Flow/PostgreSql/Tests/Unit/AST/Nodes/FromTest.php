<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\Exception\InvalidFromNodeException;
use Flow\PostgreSql\AST\Nodes\From;
use Flow\PostgreSql\AST\Nodes\Statement\SelectStatement;
use Flow\PostgreSql\AST\Nodes\Tables;
use Flow\PostgreSql\Protobuf\AST\Node;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\derived;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\sql_parse;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\table_func;

final class FromTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_accepts_join_expr_node(): void
    {
        $statement = sql_parse(
            select(star())
                ->from(table('users'))
                ->join(table('orders'), eq(col('id', 'users'), col('user_id', 'orders')))
                ->toSql(),
        )
            ->statements()
            ->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        static::assertFalse($from->isEmpty());
    }

    public function test_accepts_range_function_node(): void
    {
        $statement = sql_parse(
            select(star())->from(table_func(func('generate_series', [literal(1), literal(10)])))->toSql(),
        )
            ->statements()
            ->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        static::assertFalse($from->isEmpty());
    }

    public function test_accepts_range_subselect_node(): void
    {
        $statement = sql_parse(select(star())->from(derived(select(literal(1)), 't'))->toSql())->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        static::assertFalse($from->isEmpty());
    }

    public function test_accepts_range_var_node(): void
    {
        $statement = sql_parse(select(star())->from(table('users'))->toSql())->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        static::assertFalse($from->isEmpty());
    }

    public function test_count_returns_number_of_from_nodes(): void
    {
        $statement = sql_parse(select(star())->from(table('users'), table('orders'))->toSql())->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertCount(2, $statement->from());
    }

    public function test_count_returns_zero_for_empty_from(): void
    {
        $from = new From([]);
        static::assertCount(0, $from);
    }

    public function test_empty_nodes_array_creates_empty_from(): void
    {
        $from = new From([]);
        static::assertTrue($from->isEmpty());
    }

    public function test_has_function_returns_false_for_regular_table(): void
    {
        $statement = sql_parse(select(star())->from(table('users'))->toSql())->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->from()->hasFunction());
    }

    public function test_has_function_returns_true_for_function_in_from(): void
    {
        $statement = sql_parse(
            select(star())->from(table_func(func('generate_series', [literal(1), literal(10)])))->toSql(),
        )
            ->statements()
            ->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->from()->hasFunction());
    }

    public function test_has_function_returns_true_for_unnest_function(): void
    {
        $statement = sql_parse('SELECT * FROM unnest(ARRAY[1,2,3])')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->from()->hasFunction());
    }

    public function test_tables_returns_empty_collection_for_empty_from(): void
    {
        $from = new From([]);

        $tables = $from->tables();

        static::assertInstanceOf(Tables::class, $tables);
        static::assertTrue($tables->isEmpty());
    }

    public function test_tables_returns_empty_collection_for_function(): void
    {
        $statement = sql_parse(
            select(star())->from(table_func(func('generate_series', [literal(1), literal(10)])))->toSql(),
        )
            ->statements()
            ->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        $tables = $statement->from()->tables();

        static::assertTrue($tables->isEmpty());
    }

    public function test_tables_returns_empty_collection_for_join(): void
    {
        $statement = sql_parse(
            select(star())
                ->from(table('users'))
                ->join(table('orders'), eq(col('id', 'users'), col('user_id', 'orders')))
                ->toSql(),
        )
            ->statements()
            ->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        $tables = $statement->from()->tables();

        static::assertTrue($tables->isEmpty());
    }

    public function test_tables_returns_empty_collection_for_subquery(): void
    {
        $statement = sql_parse(select(star())->from(derived(select(literal(1)), 't'))->toSql())->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        $tables = $statement->from()->tables();

        static::assertTrue($tables->isEmpty());
    }

    public function test_tables_returns_multiple_tables(): void
    {
        $statement = sql_parse(select(star())->from(table('users'), table('orders'))->toSql())->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        $tables = $statement->from()->tables();

        static::assertCount(2, $tables);
        static::assertSame('users', $tables->first()?->name());
        static::assertSame('orders', $tables->last()?->name());
    }

    public function test_tables_returns_single_table(): void
    {
        $statement = sql_parse(select(star())->from(table('users'))->toSql())->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        $tables = $statement->from()->tables();

        static::assertTrue($tables->isSingle());
        static::assertSame('users', $tables->first()?->name());
    }

    public function test_throws_exception_for_invalid_node(): void
    {
        $invalidNode = new Node();

        $this->expectException(InvalidFromNodeException::class);
        $this->expectExceptionMessage('Invalid FROM clause node type');

        new From([$invalidNode]);
    }
}
