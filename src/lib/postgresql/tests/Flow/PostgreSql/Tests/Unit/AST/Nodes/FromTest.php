<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Nodes;

use function Flow\PostgreSql\DSL\sql_parse;

use Flow\PostgreSql\AST\Nodes\Exception\InvalidFromNodeException;
use Flow\PostgreSql\AST\Nodes\From;
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
        $statement = sql_parse('SELECT * FROM users JOIN orders ON users.id = orders.user_id')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        self::assertFalse($from->isEmpty());
    }

    public function test_accepts_range_function_node() : void
    {
        $statement = sql_parse('SELECT * FROM generate_series(1, 10)')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        self::assertFalse($from->isEmpty());
    }

    public function test_accepts_range_subselect_node() : void
    {
        $statement = sql_parse('SELECT * FROM (SELECT 1) AS t')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        self::assertFalse($from->isEmpty());
    }

    public function test_accepts_range_var_node() : void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        $from = $statement->from();
        self::assertFalse($from->isEmpty());
    }

    public function test_count_returns_number_of_from_nodes() : void
    {
        $statement = sql_parse('SELECT * FROM users, orders')->statements()->first();
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
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertFalse($statement->from()->hasFunction());
    }

    public function test_has_function_returns_true_for_function_in_from() : void
    {
        $statement = sql_parse('SELECT * FROM generate_series(1, 10)')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->from()->hasFunction());
    }

    public function test_has_function_returns_true_for_unnest_function() : void
    {
        $statement = sql_parse('SELECT * FROM unnest(ARRAY[1,2,3])')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->from()->hasFunction());
    }

    public function test_throws_exception_for_invalid_node() : void
    {
        $invalidNode = new Node();

        $this->expectException(InvalidFromNodeException::class);
        $this->expectExceptionMessage('Invalid FROM clause node type');

        new From([$invalidNode]);
    }
}
