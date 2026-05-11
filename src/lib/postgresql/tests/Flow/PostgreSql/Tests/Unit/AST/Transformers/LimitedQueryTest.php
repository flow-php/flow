<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\sql_to_limited_query;

final class LimitedQueryTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it.',
            );
        }
    }

    public function test_limit_does_not_add_offset(): void
    {
        $result = sql_to_limited_query('SELECT id, name FROM products ORDER BY name', 25);

        static::assertSame('SELECT id, name FROM products ORDER BY name LIMIT 25', $result);
        static::assertStringNotContainsString('OFFSET', $result);
    }

    public function test_limit_overrides_existing_limit(): void
    {
        $result = sql_to_limited_query('SELECT * FROM users LIMIT 100', 10);

        static::assertSame('SELECT * FROM users LIMIT 10', $result);
    }

    public function test_limit_removes_existing_offset(): void
    {
        $result = sql_to_limited_query('SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 50', 10);

        static::assertSame('SELECT * FROM users ORDER BY id LIMIT 10', $result);
        static::assertStringNotContainsString('OFFSET', $result);
    }

    public function test_limit_union_wraps_in_subquery(): void
    {
        $sql = 'SELECT id FROM users UNION SELECT id FROM admins ORDER BY id';

        $result = sql_to_limited_query($sql, 10);

        static::assertSame(
            'SELECT * FROM (SELECT id FROM users UNION SELECT id FROM admins ORDER BY id) _pagination_subq LIMIT 10',
            $result,
        );
        static::assertStringNotContainsString('OFFSET', $result);
    }

    public function test_limit_with_joins(): void
    {
        $sql = 'SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id';

        $result = sql_to_limited_query($sql, 50);

        static::assertSame('SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id LIMIT 50', $result);
    }

    public function test_limit_with_where_clause(): void
    {
        $result = sql_to_limited_query('SELECT * FROM users WHERE active = true', 5);

        static::assertSame('SELECT * FROM users WHERE active = true LIMIT 5', $result);
    }

    public function test_simple_select_with_limit(): void
    {
        $result = sql_to_limited_query('SELECT * FROM users', 10);

        static::assertSame('SELECT * FROM users LIMIT 10', $result);
    }
}
