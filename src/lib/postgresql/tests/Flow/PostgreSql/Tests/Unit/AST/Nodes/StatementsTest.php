<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\Exception\InvalidStatementException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\sql_parse;

final class StatementsTest extends TestCase
{
    /**
     * @return Generator<string, array{string}>
     */
    public static function provide_not_one_select(): Generator
    {
        yield 'insert returning' => ['INSERT INTO t VALUES (1) RETURNING id'];
        yield 'update returning' => ['UPDATE t SET id = 1 RETURNING id'];
        yield 'explain' => ['EXPLAIN SELECT 1'];
        yield 'two selects' => ['SELECT 1; SELECT 2'];
        yield 'a select, then a write' => ['SELECT 1; DELETE FROM t'];
    }

    /**
     * @return Generator<string, array{string}>
     */
    public static function provide_read_only_selects(): Generator
    {
        yield 'select' => ['SELECT id FROM t'];
        yield 'values' => ['VALUES (1), (2)'];
        yield 'read-only cte' => ['WITH x AS (SELECT 1 AS id) SELECT id FROM x'];
        yield 'union' => ['SELECT 1 UNION SELECT 2'];
    }

    /**
     * @return Generator<string, array{string}>
     */
    public static function provide_selects_that_write(): Generator
    {
        yield 'data-modifying cte' => ['WITH x AS (INSERT INTO t VALUES (1) RETURNING id) SELECT id FROM x'];
        yield 'select into' => ['SELECT id INTO copy FROM t'];
        yield 'select into on the first arm of a union' => ['SELECT id INTO copy FROM t UNION SELECT 1'];
    }

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    #[DataProvider('provide_selects_that_write')]
    public function test_assert_read_only_select_refuses_a_select_that_writes(string $sql): void
    {
        $this->expectException(InvalidStatementException::class);
        $this->expectExceptionMessage(
            'Expected a read-only SELECT - the query holds a data-modifying WITH or SELECT ... INTO',
        );

        sql_parse($sql)->statements()->assertReadOnlySelect();
    }

    #[DataProvider('provide_not_one_select')]
    public function test_assert_read_only_select_refuses_anything_but_one_select(string $sql): void
    {
        $this->expectException(InvalidStatementException::class);
        $this->expectExceptionMessage('Expected exactly one SELECT or VALUES statement');

        sql_parse($sql)->statements()->assertReadOnlySelect();
    }

    #[DataProvider('provide_read_only_selects')]
    public function test_assert_read_only_select_returns_the_select(string $sql): void
    {
        $statements = sql_parse($sql)->statements();

        static::assertSame($statements->first(), $statements->assertReadOnlySelect());
    }
}
