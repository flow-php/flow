<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Extractors;

use Flow\PostgreSql\Extractors\QueryDepth;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\sql_parse;
use function Flow\PostgreSql\DSL\sql_query_depth;

final class QueryDepthTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_depth_for_cte_query(): void
    {
        $depth = new QueryDepth(sql_parse('WITH cte AS (SELECT * FROM t) SELECT * FROM cte'));

        static::assertSame(2, $depth->depth());
    }

    public function test_depth_for_deeply_nested_query(): void
    {
        $depth = new QueryDepth(sql_parse(
            'SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM t) AS a) AS b) AS c',
        ));

        static::assertSame(4, $depth->depth());
    }

    public function test_depth_for_delete_with_subquery(): void
    {
        $depth = new QueryDepth(sql_parse('DELETE FROM t WHERE id IN (SELECT id FROM users)'));

        static::assertSame(1, $depth->depth());
    }

    public function test_depth_for_insert_select(): void
    {
        $depth = new QueryDepth(sql_parse('INSERT INTO t SELECT * FROM users'));

        static::assertSame(1, $depth->depth());
    }

    public function test_depth_for_insert_select_with_subquery(): void
    {
        $depth = new QueryDepth(sql_parse('INSERT INTO t SELECT * FROM (SELECT * FROM users) AS sub'));

        static::assertSame(2, $depth->depth());
    }

    public function test_depth_for_simple_select(): void
    {
        $depth = new QueryDepth(sql_parse('SELECT * FROM users'));

        static::assertSame(1, $depth->depth());
    }

    public function test_depth_for_subquery_in_from(): void
    {
        $depth = new QueryDepth(sql_parse('SELECT * FROM (SELECT * FROM t) AS sub'));

        static::assertSame(2, $depth->depth());
    }

    public function test_depth_for_subquery_in_where(): void
    {
        $depth = new QueryDepth(sql_parse('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)'));

        static::assertSame(2, $depth->depth());
    }

    public function test_depth_for_update_with_subquery(): void
    {
        $depth = new QueryDepth(sql_parse('UPDATE t SET x = 1 WHERE id IN (SELECT id FROM users)'));

        static::assertSame(1, $depth->depth());
    }

    public function test_dsl_function_returns_correct_depth(): void
    {
        static::assertSame(1, sql_query_depth('SELECT * FROM t'));
        static::assertSame(2, sql_query_depth('SELECT * FROM (SELECT * FROM t) AS sub'));
        static::assertSame(
            4,
            sql_query_depth('SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM t) AS a) AS b) AS c'),
        );
    }
}
