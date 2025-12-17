<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Nodes\Statement;

use function Flow\PostgreSql\DSL\sql_parse;

use Flow\PostgreSql\AST\Nodes\Statement\SelectStatement;
use PHPUnit\Framework\TestCase;

final class SelectStatementTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_from_has_values_returns_false_when_from_regular_table() : void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertFalse($statement->from()->hasValues());
    }

    public function test_from_has_values_returns_false_when_from_subquery() : void
    {
        $statement = sql_parse('SELECT * FROM (SELECT id FROM users) AS t')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertFalse($statement->from()->hasValues());
    }

    public function test_from_has_values_returns_true_when_values_used() : void
    {
        $statement = sql_parse('SELECT * FROM (VALUES (1, 2), (3, 4)) AS t')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->from()->hasValues());
    }

    public function test_from_has_values_returns_true_when_values_with_column_names() : void
    {
        $statement = sql_parse('SELECT * FROM (VALUES (1, 2), (3, 4)) AS t(a, b)')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->from()->hasValues());
    }

    public function test_from_is_empty_returns_false_when_from_clause_exists() : void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertFalse($statement->from()->isEmpty());
    }

    public function test_from_is_empty_returns_true_when_no_from_clause() : void
    {
        $statement = sql_parse('SELECT 1')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->from()->isEmpty());
    }

    public function test_has_cte_returns_false_when_no_with_clause() : void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertFalse($statement->hasCte());
    }

    public function test_has_cte_returns_true_when_with_clause_exists() : void
    {
        $statement = sql_parse('WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->hasCte());
    }

    public function test_has_into_clause_returns_false_when_no_into_clause() : void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertFalse($statement->hasIntoClause());
    }

    public function test_has_into_clause_returns_true_when_into_clause_exists() : void
    {
        $statement = sql_parse('SELECT * INTO new_users FROM users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->hasIntoClause());
    }

    public function test_has_limit_returns_false_when_no_limit() : void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertFalse($statement->hasLimit());
    }

    public function test_has_limit_returns_true_when_limit_exists() : void
    {
        $statement = sql_parse('SELECT * FROM users LIMIT 10')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->hasLimit());
    }

    public function test_has_locking_clause_returns_false_when_no_locking() : void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertFalse($statement->hasLockingClause());
    }

    public function test_has_locking_clause_returns_true_when_for_share_exists() : void
    {
        $statement = sql_parse('SELECT * FROM users FOR SHARE')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->hasLockingClause());
    }

    public function test_has_locking_clause_returns_true_when_for_update_exists() : void
    {
        $statement = sql_parse('SELECT * FROM users FOR UPDATE')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->hasLockingClause());
    }

    public function test_has_offset_returns_false_when_no_offset() : void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertFalse($statement->hasOffset());
    }

    public function test_has_offset_returns_true_when_limit_and_offset_exist() : void
    {
        $statement = sql_parse('SELECT * FROM users LIMIT 10 OFFSET 5')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->hasLimit());
        self::assertTrue($statement->hasOffset());
    }

    public function test_has_offset_returns_true_when_offset_exists() : void
    {
        $statement = sql_parse('SELECT * FROM users OFFSET 5')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->hasOffset());
    }

    public function test_has_set_operation_returns_false_when_no_set_operation() : void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertFalse($statement->hasSetOperation());
    }

    public function test_has_set_operation_returns_true_when_except_exists() : void
    {
        $statement = sql_parse('SELECT * FROM t1 EXCEPT SELECT * FROM t2')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->hasSetOperation());
    }

    public function test_has_set_operation_returns_true_when_intersect_exists() : void
    {
        $statement = sql_parse('SELECT * FROM t1 INTERSECT SELECT * FROM t2')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->hasSetOperation());
    }

    public function test_has_set_operation_returns_true_when_union_exists() : void
    {
        $statement = sql_parse('SELECT * FROM t1 UNION SELECT * FROM t2')->statements()->first();
        self::assertInstanceOf(SelectStatement::class, $statement);

        self::assertTrue($statement->hasSetOperation());
    }
}
