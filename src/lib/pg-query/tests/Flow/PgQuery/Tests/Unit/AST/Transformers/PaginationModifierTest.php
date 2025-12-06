<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\AST\Transformers;

use function Flow\PgQuery\DSL\sql_parse;
use Flow\PgQuery\AST\Transformers\{PaginationConfig, PaginationModifier};
use Flow\PgQuery\AST\Traverser;
use Flow\PgQuery\Exception\PaginationException;
use PHPUnit\Framework\TestCase;

final class PaginationModifierTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_add_pagination_to_simple_select() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY id');

        $modifier = new PaginationModifier(new PaginationConfig(10, 5));

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 5', $parsed->deparse());
    }

    public function test_add_pagination_without_offset() : void
    {
        $parsed = sql_parse('SELECT id, name FROM users WHERE active = true');

        $modifier = new PaginationModifier(new PaginationConfig(20));

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT id, name FROM users WHERE active = true LIMIT 20', $parsed->deparse());
    }

    public function test_clears_existing_offset_when_new_offset_is_zero() : void
    {
        $parsed = sql_parse('SELECT * FROM users LIMIT 100 OFFSET 50');

        $modifier = new PaginationModifier(new PaginationConfig(10, 0));
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users LIMIT 10', $parsed->deparse());
    }

    public function test_cte_main_query_gets_paginated() : void
    {
        $parsed = sql_parse('WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active');

        $modifier = new PaginationModifier(new PaginationConfig(10));

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active LIMIT 10', $parsed->deparse());
    }

    public function test_deeply_nested_subqueries() : void
    {
        $parsed = sql_parse('SELECT * FROM (SELECT * FROM (SELECT id FROM users) AS inner1) AS outer1');

        $modifier = new PaginationModifier(new PaginationConfig(10));
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM (SELECT * FROM (SELECT id FROM users) inner1) outer1 LIMIT 10', $parsed->deparse());
    }

    public function test_distinct_query() : void
    {
        $parsed = sql_parse('SELECT DISTINCT name FROM users');

        $modifier = new PaginationModifier(new PaginationConfig(10));

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT DISTINCT name FROM users LIMIT 10', $parsed->deparse());
    }

    public function test_except_query_wraps_in_subquery() : void
    {
        $parsed = sql_parse('SELECT id FROM users EXCEPT SELECT id FROM banned');

        $modifier = new PaginationModifier(new PaginationConfig(15));

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM (SELECT id FROM users EXCEPT SELECT id FROM banned) _pagination_subq LIMIT 15', $parsed->deparse());
    }

    public function test_group_by_having_query() : void
    {
        $parsed = sql_parse('SELECT dept, COUNT(*) as cnt FROM users GROUP BY dept HAVING COUNT(*) > 5');

        $modifier = new PaginationModifier(new PaginationConfig(10));

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT dept, count(*) AS cnt FROM users GROUP BY dept HAVING count(*) > 5 LIMIT 10', $parsed->deparse());
    }

    public function test_inner_join() : void
    {
        $parsed = sql_parse('SELECT u.id, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id');

        $modifier = new PaginationModifier(new PaginationConfig(10));
        $parsed->traverse($modifier);

        self::assertSame('SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.user_id LIMIT 10', $parsed->deparse());
    }

    public function test_intersect_query_wraps_in_subquery() : void
    {
        $parsed = sql_parse('SELECT id FROM users INTERSECT SELECT id FROM admins ORDER BY id');

        $modifier = new PaginationModifier(new PaginationConfig(5, 2));

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM (SELECT id FROM users INTERSECT SELECT id FROM admins ORDER BY id) _pagination_subq LIMIT 5 OFFSET 2', $parsed->deparse());
    }

    public function test_lateral_join() : void
    {
        $parsed = sql_parse('SELECT u.id, latest.amount FROM users u, LATERAL (SELECT amount FROM orders WHERE user_id = u.id ORDER BY created_at DESC LIMIT 1) AS latest');

        $modifier = new PaginationModifier(new PaginationConfig(10));
        $parsed->traverse($modifier);

        self::assertSame('SELECT u.id, latest.amount FROM users u, LATERAL (SELECT amount FROM orders WHERE user_id = u.id ORDER BY created_at DESC LIMIT 1) latest LIMIT 10', $parsed->deparse());
    }

    public function test_left_join() : void
    {
        $parsed = sql_parse('SELECT u.id, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id');

        $modifier = new PaginationModifier(new PaginationConfig(10));
        $parsed->traverse($modifier);

        self::assertSame('SELECT u.id, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id LIMIT 10', $parsed->deparse());
    }

    public function test_multiple_joins() : void
    {
        $parsed = sql_parse('SELECT u.id, o.amount, p.name FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id');

        $modifier = new PaginationModifier(new PaginationConfig(10));
        $parsed->traverse($modifier);

        self::assertSame('SELECT u.id, o.amount, p.name FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id LIMIT 10', $parsed->deparse());
    }

    public function test_nested_union() : void
    {
        $parsed = sql_parse('(SELECT id FROM users UNION SELECT id FROM admins) UNION SELECT id FROM guests');

        $modifier = new PaginationModifier(new PaginationConfig(10));
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM ((SELECT id FROM users UNION SELECT id FROM admins) UNION SELECT id FROM guests) _pagination_subq LIMIT 10', $parsed->deparse());
    }

    public function test_offset_with_order_by_works() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY name ASC');

        $modifier = new PaginationModifier(new PaginationConfig(10, 20));

        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users ORDER BY name ASC LIMIT 10 OFFSET 20', $parsed->deparse());
    }

    public function test_offset_without_order_by_throws_exception() : void
    {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage('OFFSET without ORDER BY produces non-deterministic results');

        $parsed = sql_parse('SELECT * FROM users');

        $modifier = new PaginationModifier(new PaginationConfig(10, 5));

        $parsed->traverse($modifier);
    }

    public function test_order_by_query() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY name ASC');

        $modifier = new PaginationModifier(new PaginationConfig(10));

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM users ORDER BY name ASC LIMIT 10', $parsed->deparse());
    }

    public function test_overrides_existing_limit() : void
    {
        $parsed = sql_parse('SELECT * FROM users LIMIT 100');

        $modifier = new PaginationModifier(new PaginationConfig(10));

        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users LIMIT 10', $parsed->deparse());
    }

    public function test_overrides_existing_limit_and_offset() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 50');

        $modifier = new PaginationModifier(new PaginationConfig(10, 5));

        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 5', $parsed->deparse());
    }

    public function test_pagination_config_default_offset() : void
    {
        $config = new PaginationConfig(20);

        self::assertSame(20, $config->limit);
        self::assertSame(0, $config->offset);
    }

    public function test_pagination_config_returns_config_object() : void
    {
        $config = new PaginationConfig(10, 5);

        self::assertSame(10, $config->limit);
        self::assertSame(5, $config->offset);
    }

    public function test_self_join() : void
    {
        $parsed = sql_parse('SELECT e.name AS employee, m.name AS manager FROM employees e LEFT JOIN employees m ON e.manager_id = m.id');

        $modifier = new PaginationModifier(new PaginationConfig(10));
        $parsed->traverse($modifier);

        self::assertSame('SELECT e.name AS employee, m.name AS manager FROM employees e LEFT JOIN employees m ON e.manager_id = m.id LIMIT 10', $parsed->deparse());
    }

    public function test_subquery_in_from_clause_not_paginated() : void
    {
        $parsed = sql_parse('SELECT * FROM (SELECT id FROM users) AS sub');

        $modifier = new PaginationModifier(new PaginationConfig(10));

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM (SELECT id FROM users) sub LIMIT 10', $parsed->deparse());
    }

    public function test_subquery_in_where_clause_not_paginated() : void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)');

        $modifier = new PaginationModifier(new PaginationConfig(10));

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) LIMIT 10', $parsed->deparse());
    }

    public function test_union_all() : void
    {
        $parsed = sql_parse('SELECT id FROM users UNION ALL SELECT id FROM admins');

        $modifier = new PaginationModifier(new PaginationConfig(10));
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM (SELECT id FROM users UNION ALL SELECT id FROM admins) _pagination_subq LIMIT 10', $parsed->deparse());
    }

    public function test_union_query_wraps_in_subquery() : void
    {
        $parsed = sql_parse('SELECT id FROM users UNION SELECT id FROM admins');

        $modifier = new PaginationModifier(new PaginationConfig(10));

        $traverser = new Traverser($modifier);
        $traverser->traverse($parsed->raw());

        self::assertSame('SELECT * FROM (SELECT id FROM users UNION SELECT id FROM admins) _pagination_subq LIMIT 10', $parsed->deparse());
    }

    public function test_values_clause() : void
    {
        $parsed = sql_parse('SELECT * FROM (VALUES (1, \'a\'), (2, \'b\'), (3, \'c\')) AS t(id, name)');

        $modifier = new PaginationModifier(new PaginationConfig(2));
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM (VALUES (1, \'a\'), (2, \'b\'), (3, \'c\')) t(id, name) LIMIT 2', $parsed->deparse());
    }

    public function test_window_function() : void
    {
        $parsed = sql_parse('SELECT id, name, ROW_NUMBER() OVER (ORDER BY created_at) as rn FROM users');

        $modifier = new PaginationModifier(new PaginationConfig(10));
        $parsed->traverse($modifier);

        self::assertSame('SELECT id, name, row_number() OVER (ORDER BY created_at) AS rn FROM users LIMIT 10', $parsed->deparse());
    }

    public function test_window_function_with_partition() : void
    {
        $parsed = sql_parse('SELECT id, dept, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rank FROM employees');

        $modifier = new PaginationModifier(new PaginationConfig(10));
        $parsed->traverse($modifier);

        self::assertSame('SELECT id, dept, rank() OVER (PARTITION BY dept ORDER BY salary DESC) AS rank FROM employees LIMIT 10', $parsed->deparse());
    }

    public function test_with_pagination_helper_method() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY id');

        $modifier = new PaginationModifier(new PaginationConfig(25, 50));

        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users ORDER BY id LIMIT 25 OFFSET 50', $parsed->deparse());
    }
}
