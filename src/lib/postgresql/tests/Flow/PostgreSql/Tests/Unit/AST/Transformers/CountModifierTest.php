<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use Flow\PostgreSql\AST\Transformers\CountModifier;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\sql_parse;

final class CountModifierTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_count_cte_query(): void
    {
        $parsed = sql_parse(
            'WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active ORDER BY name',
        );

        $modifier = new CountModifier();
        $parsed->traverse($modifier);

        static::assertSame(
            'SELECT count(*) FROM (WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active) _count_subq',
            $parsed->deparse(),
        );
    }

    public function test_count_distinct_query(): void
    {
        $parsed = sql_parse('SELECT DISTINCT name FROM users');

        $modifier = new CountModifier();
        $parsed->traverse($modifier);

        static::assertSame('SELECT count(*) FROM (SELECT DISTINCT name FROM users) _count_subq', $parsed->deparse());
    }

    public function test_count_except_query(): void
    {
        $parsed = sql_parse('SELECT id FROM users EXCEPT SELECT id FROM banned ORDER BY id');

        $modifier = new CountModifier();
        $parsed->traverse($modifier);

        static::assertSame(
            'SELECT count(*) FROM (SELECT id FROM users EXCEPT SELECT id FROM banned) _count_subq',
            $parsed->deparse(),
        );
    }

    public function test_count_group_by_query(): void
    {
        $parsed = sql_parse('SELECT dept, COUNT(*) as cnt FROM users GROUP BY dept HAVING COUNT(*) > 5 ORDER BY dept');

        $modifier = new CountModifier();
        $parsed->traverse($modifier);

        static::assertSame(
            'SELECT count(*) FROM (SELECT dept, count(*) AS cnt FROM users GROUP BY dept HAVING count(*) > 5) _count_subq',
            $parsed->deparse(),
        );
    }

    public function test_count_intersect_query(): void
    {
        $parsed = sql_parse('SELECT id FROM users INTERSECT SELECT id FROM admins ORDER BY id');

        $modifier = new CountModifier();
        $parsed->traverse($modifier);

        static::assertSame(
            'SELECT count(*) FROM (SELECT id FROM users INTERSECT SELECT id FROM admins) _count_subq',
            $parsed->deparse(),
        );
    }

    public function test_count_removes_order_by(): void
    {
        $parsed = sql_parse('SELECT id, name FROM users WHERE active = true ORDER BY name ASC, id DESC');

        $modifier = new CountModifier();
        $parsed->traverse($modifier);

        static::assertSame(
            'SELECT count(*) FROM (SELECT id, name FROM users WHERE active = true) _count_subq',
            $parsed->deparse(),
        );
    }

    public function test_count_simple_select(): void
    {
        $parsed = sql_parse('SELECT * FROM users');

        $modifier = new CountModifier();
        $parsed->traverse($modifier);

        static::assertSame('SELECT count(*) FROM (SELECT * FROM users) _count_subq', $parsed->deparse());
    }

    public function test_count_strips_existing_limit_and_offset(): void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 50');

        $modifier = new CountModifier();
        $parsed->traverse($modifier);

        static::assertSame('SELECT count(*) FROM (SELECT * FROM users) _count_subq', $parsed->deparse());
    }

    public function test_count_union_query(): void
    {
        $parsed = sql_parse('SELECT id FROM users UNION SELECT id FROM admins ORDER BY id');

        $modifier = new CountModifier();
        $parsed->traverse($modifier);

        static::assertSame(
            'SELECT count(*) FROM (SELECT id FROM users UNION SELECT id FROM admins) _count_subq',
            $parsed->deparse(),
        );
    }

    public function test_subquery_in_from_clause_not_counted(): void
    {
        $parsed = sql_parse('SELECT * FROM (SELECT id FROM users) AS sub ORDER BY id');

        $modifier = new CountModifier();
        $parsed->traverse($modifier);

        static::assertSame(
            'SELECT count(*) FROM (SELECT * FROM (SELECT id FROM users) sub) _count_subq',
            $parsed->deparse(),
        );
    }

    public function test_subquery_in_where_clause_not_counted(): void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY id');

        $modifier = new CountModifier();
        $parsed->traverse($modifier);

        static::assertSame(
            'SELECT count(*) FROM (SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)) _count_subq',
            $parsed->deparse(),
        );
    }
}
