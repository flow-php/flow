<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement\SelectStatement;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\sql_parse;

final class SelectStatementTest extends TestCase
{
    /**
     * @return Generator<string, array{string}>
     */
    public static function provide_read_only_queries(): Generator
    {
        yield 'no with' => ['SELECT * FROM users'];
        yield 'select cte' => ['WITH x AS (SELECT 1 AS id) SELECT id FROM x'];
        yield 'values cte' => ['WITH x(id) AS (VALUES (1)) SELECT id FROM x'];
        yield 'recursive cte' => [
            'WITH RECURSIVE s(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM s WHERE id < 3) SELECT id FROM s',
        ];
        yield 'cte inside a subquery' => ['SELECT id FROM (WITH y AS (SELECT 1 AS id) SELECT id FROM y) z'];
    }

    /**
     * @return Generator<string, array{string}>
     */
    public static function provide_writing_ctes(): Generator
    {
        yield 'insert' => ['WITH x AS (INSERT INTO t VALUES (1) RETURNING id) SELECT id FROM x'];
        yield 'update' => ['WITH x AS (UPDATE t SET id = 1 RETURNING id) SELECT id FROM x'];
        yield 'delete' => ['WITH x AS (DELETE FROM t RETURNING id) SELECT id FROM x'];
        yield 'merge' => [
            'WITH x AS (MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE RETURNING t.id) SELECT id FROM x',
        ];
        yield 'second cte writes' => ['WITH a AS (SELECT 1 AS id), b AS (DELETE FROM t RETURNING id) SELECT id FROM a'];
    }

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_from_has_values_returns_false_when_from_regular_table(): void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->from()->hasValues());
    }

    public function test_from_has_values_returns_false_when_from_subquery(): void
    {
        $statement = sql_parse('SELECT * FROM (SELECT id FROM users) AS t')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->from()->hasValues());
    }

    public function test_from_has_values_returns_true_when_values_used(): void
    {
        $statement = sql_parse('SELECT * FROM (VALUES (1, 2), (3, 4)) AS t')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->from()->hasValues());
    }

    public function test_from_has_values_returns_true_when_values_with_column_names(): void
    {
        $statement = sql_parse('SELECT * FROM (VALUES (1, 2), (3, 4)) AS t(a, b)')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->from()->hasValues());
    }

    public function test_from_is_empty_returns_false_when_from_clause_exists(): void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->from()->isEmpty());
    }

    public function test_from_is_empty_returns_true_when_no_from_clause(): void
    {
        $statement = sql_parse('SELECT 1')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->from()->isEmpty());
    }

    public function test_has_cte_returns_false_when_no_with_clause(): void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->hasCte());
    }

    public function test_has_cte_returns_true_when_with_clause_exists(): void
    {
        $statement = sql_parse(
            'WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users',
        )
            ->statements()
            ->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasCte());
    }

    #[DataProvider('provide_read_only_queries')]
    public function test_has_data_modifying_cte_returns_false_for_a_read_only_query(string $sql): void
    {
        $statement = sql_parse($sql)->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->hasDataModifyingCte());
    }

    #[DataProvider('provide_writing_ctes')]
    public function test_has_data_modifying_cte_returns_true_when_a_cte_writes(string $sql): void
    {
        $statement = sql_parse($sql)->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasDataModifyingCte());
    }

    #[TestWith(['SELECT id INTO TEMPORARY n FROM t UNION SELECT id FROM t'])]
    #[TestWith(['SELECT id INTO TEMPORARY n FROM t UNION SELECT 1 UNION SELECT 2'])]
    #[TestWith(['SELECT id INTO TEMPORARY n FROM t EXCEPT SELECT 1'])]
    public function test_has_into_clause_returns_true_when_the_first_select_of_a_set_operation_has_it(string $sql): void
    {
        $statement = sql_parse($sql)->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasIntoClause());
    }

    public function test_has_into_clause_returns_false_when_no_into_clause(): void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->hasIntoClause());
    }

    public function test_has_into_clause_returns_true_when_into_clause_exists(): void
    {
        $statement = sql_parse('SELECT * INTO new_users FROM users')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasIntoClause());
    }

    public function test_has_limit_returns_false_when_no_limit(): void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->hasLimit());
    }

    public function test_has_limit_returns_true_when_limit_exists(): void
    {
        $statement = sql_parse('SELECT * FROM users LIMIT 10')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasLimit());
    }

    public function test_has_locking_clause_returns_false_when_no_locking(): void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->hasLockingClause());
    }

    public function test_has_locking_clause_returns_true_when_for_share_exists(): void
    {
        $statement = sql_parse('SELECT * FROM users FOR SHARE')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasLockingClause());
    }

    public function test_has_locking_clause_returns_true_when_for_update_exists(): void
    {
        $statement = sql_parse('SELECT * FROM users FOR UPDATE')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasLockingClause());
    }

    public function test_has_offset_returns_false_when_no_offset(): void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->hasOffset());
    }

    public function test_has_offset_returns_true_when_limit_and_offset_exist(): void
    {
        $statement = sql_parse('SELECT * FROM users LIMIT 10 OFFSET 5')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasLimit());
        static::assertTrue($statement->hasOffset());
    }

    public function test_has_offset_returns_true_when_offset_exists(): void
    {
        $statement = sql_parse('SELECT * FROM users OFFSET 5')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasOffset());
    }

    #[TestWith(['SELECT * FROM users'])]
    #[TestWith(['SELECT * FROM (SELECT id FROM users ORDER BY id) sub'])]
    #[TestWith(['SELECT id, row_number() OVER (ORDER BY id) FROM users'])]
    public function test_has_order_by_returns_false_without_a_top_level_order_by(string $sql): void
    {
        $statement = sql_parse($sql)->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->hasOrderBy());
    }

    #[TestWith(['SELECT * FROM users ORDER BY id'])]
    #[TestWith(['SELECT id FROM users UNION SELECT id FROM admins ORDER BY id'])]
    public function test_has_order_by_returns_true_with_a_top_level_order_by(string $sql): void
    {
        $statement = sql_parse($sql)->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasOrderBy());
    }

    public function test_has_set_operation_returns_false_when_no_set_operation(): void
    {
        $statement = sql_parse('SELECT * FROM users')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertFalse($statement->hasSetOperation());
    }

    public function test_has_set_operation_returns_true_when_except_exists(): void
    {
        $statement = sql_parse('SELECT * FROM t1 EXCEPT SELECT * FROM t2')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasSetOperation());
    }

    public function test_has_set_operation_returns_true_when_intersect_exists(): void
    {
        $statement = sql_parse('SELECT * FROM t1 INTERSECT SELECT * FROM t2')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasSetOperation());
    }

    public function test_has_set_operation_returns_true_when_union_exists(): void
    {
        $statement = sql_parse('SELECT * FROM t1 UNION SELECT * FROM t2')->statements()->first();
        static::assertInstanceOf(SelectStatement::class, $statement);

        static::assertTrue($statement->hasSetOperation());
    }
}
