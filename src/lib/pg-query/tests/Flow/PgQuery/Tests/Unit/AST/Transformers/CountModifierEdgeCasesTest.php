<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\AST\Transformers;

use function Flow\PgQuery\DSL\{pg_to_count_query, pg_to_paginated_query};

use PHPUnit\Framework\TestCase;

final class CountModifierEdgeCasesTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_complete_paginator_pattern() : void
    {
        $baseQuery = <<<'SQL'
            SELECT
                p.id,
                p.name,
                p.price,
                c.name AS category_name
            FROM products p
            JOIN categories c ON p.category_id = c.id
            WHERE p.active = true
                AND p.price >= 10
            ORDER BY p.name ASC, p.id
            SQL;

        $countQuery = pg_to_count_query($baseQuery);
        self::assertSame(
            'SELECT count(*) FROM (SELECT p.id, p.name, p.price, c.name AS category_name FROM products p JOIN categories c ON p.category_id = c.id WHERE p.active = true AND p.price >= 10) _count_subq',
            $countQuery
        );

        $page1 = pg_to_paginated_query($baseQuery, limit: 25, offset: 0);
        self::assertSame(
            'SELECT p.id, p.name, p.price, c.name AS category_name FROM products p JOIN categories c ON p.category_id = c.id WHERE p.active = true AND p.price >= 10 ORDER BY p.name ASC, p.id LIMIT 25',
            $page1
        );

        $page2 = pg_to_paginated_query($baseQuery, limit: 25, offset: 25);
        self::assertSame(
            'SELECT p.id, p.name, p.price, c.name AS category_name FROM products p JOIN categories c ON p.category_id = c.id WHERE p.active = true AND p.price >= 10 ORDER BY p.name ASC, p.id LIMIT 25 OFFSET 25',
            $page2
        );
    }

    public function test_count_with_aggregation() : void
    {
        $baseQuery = <<<'SQL'
            SELECT
                category_id,
                COUNT(*) AS product_count,
                AVG(price) AS avg_price
            FROM products
            WHERE active = true
            GROUP BY category_id
            HAVING COUNT(*) >= 5
            ORDER BY product_count DESC
            SQL;

        $countQuery = pg_to_count_query($baseQuery);

        self::assertSame(
            'SELECT count(*) FROM (SELECT category_id, count(*) AS product_count, avg(price) AS avg_price FROM products WHERE active = true GROUP BY category_id HAVING count(*) >= 5) _count_subq',
            $countQuery
        );
    }

    public function test_count_with_complex_joins() : void
    {
        $baseQuery = <<<'SQL'
            SELECT
                o.id,
                o.total_amount,
                u.email,
                p.name AS product_name
            FROM orders o
            JOIN users u ON o.user_id = u.id
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON oi.product_id = p.id
            WHERE o.status = 'completed'
                AND o.created_at >= '2025-01-01'
            ORDER BY o.created_at DESC, o.id
            SQL;

        $countQuery = pg_to_count_query($baseQuery);

        self::assertSame(
            'SELECT count(*) FROM (SELECT o.id, o.total_amount, u.email, p.name AS product_name FROM orders o JOIN users u ON o.user_id = u.id JOIN order_items oi ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id WHERE o.status = \'completed\' AND o.created_at >= \'2025-01-01\') _count_subq',
            $countQuery
        );
    }

    public function test_count_with_cte() : void
    {
        $baseQuery = <<<'SQL'
            WITH active_customers AS (
                SELECT DISTINCT u.id, u.email
                FROM users u
                JOIN orders o ON u.id = o.user_id
                WHERE o.created_at >= NOW() - INTERVAL '30 days'
            )
            SELECT ac.id, ac.email, COUNT(o.id) AS order_count
            FROM active_customers ac
            JOIN orders o ON ac.id = o.user_id
            GROUP BY ac.id, ac.email
            ORDER BY order_count DESC
            SQL;

        $countQuery = pg_to_count_query($baseQuery);

        self::assertSame(
            'SELECT count(*) FROM (WITH active_customers AS (SELECT DISTINCT u.id, u.email FROM users u JOIN orders o ON u.id = o.user_id WHERE o.created_at >= (now() - \'30 days\'::interval)) SELECT ac.id, ac.email, count(o.id) AS order_count FROM active_customers ac JOIN orders o ON ac.id = o.user_id GROUP BY ac.id, ac.email) _count_subq',
            $countQuery
        );
    }

    public function test_count_with_distinct() : void
    {
        $baseQuery = <<<'SQL'
            SELECT DISTINCT
                u.id,
                u.email,
                u.name
            FROM users u
            JOIN orders o ON u.id = o.user_id
            WHERE o.total_amount > 100
            ORDER BY u.name
            SQL;

        $countQuery = pg_to_count_query($baseQuery);

        self::assertSame(
            'SELECT count(*) FROM (SELECT DISTINCT u.id, u.email, u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total_amount > 100) _count_subq',
            $countQuery
        );
    }

    public function test_count_with_subquery_in_where() : void
    {
        $baseQuery = <<<'SQL'
            SELECT
                p.id,
                p.name,
                p.price
            FROM products p
            WHERE p.category_id IN (
                SELECT c.id
                FROM categories c
                WHERE c.active = true
                    AND c.parent_id IS NOT NULL
            )
            AND p.price > (
                SELECT AVG(price)
                FROM products
                WHERE active = true
            )
            ORDER BY p.price DESC
            SQL;

        $countQuery = pg_to_count_query($baseQuery);

        self::assertSame(
            'SELECT count(*) FROM (SELECT p.id, p.name, p.price FROM products p WHERE p.category_id IN (SELECT c.id FROM categories c WHERE c.active = true AND c.parent_id IS NOT NULL) AND p.price > (SELECT avg(price) FROM products WHERE active = true)) _count_subq',
            $countQuery
        );
    }

    public function test_count_with_union() : void
    {
        $baseQuery = <<<'SQL'
            SELECT id, 'order' AS notification_type, message FROM order_notifications WHERE read = false
            UNION ALL
            SELECT id, 'payment' AS notification_type, message FROM payment_notifications WHERE read = false
            UNION ALL
            SELECT id, 'shipping' AS notification_type, message FROM shipping_notifications WHERE read = false
            ORDER BY id DESC
            SQL;

        $countQuery = pg_to_count_query($baseQuery);

        self::assertSame(
            'SELECT count(*) FROM ((SELECT id, \'order\' AS notification_type, message FROM order_notifications WHERE read = false UNION ALL SELECT id, \'payment\' AS notification_type, message FROM payment_notifications WHERE read = false) UNION ALL SELECT id, \'shipping\' AS notification_type, message FROM shipping_notifications WHERE read = false) _count_subq',
            $countQuery
        );
    }

    public function test_count_with_window_functions() : void
    {
        $baseQuery = <<<'SQL'
            SELECT
                e.id,
                e.name,
                e.salary,
                e.department_id,
                RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS dept_rank
            FROM employees e
            WHERE e.active = true
            ORDER BY e.department_id, dept_rank
            SQL;

        $countQuery = pg_to_count_query($baseQuery);

        self::assertSame(
            'SELECT count(*) FROM (SELECT e.id, e.name, e.salary, e.department_id, rank() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS dept_rank FROM employees e WHERE e.active = true) _count_subq',
            $countQuery
        );
    }
}
