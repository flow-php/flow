<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\sql_to_paginated_query;

final class OffsetPaginationEdgeCasesTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_complex_aggregation_pagination(): void
    {
        $baseQuery = <<<'SQL'
            SELECT
                category_id,
                COUNT(*) AS product_count,
                AVG(price) AS avg_price,
                MIN(price) AS min_price,
                MAX(price) AS max_price
            FROM products
            WHERE active = true
            GROUP BY category_id
            HAVING COUNT(*) >= 5
            ORDER BY product_count DESC, category_id
            SQL;

        $page1 = sql_to_paginated_query($baseQuery, limit: 10, offset: 0);
        static::assertSame(
            'SELECT category_id, count(*) AS product_count, avg(price) AS avg_price, min(price) AS min_price, max(price) AS max_price FROM products WHERE active = true GROUP BY category_id HAVING count(*) >= 5 ORDER BY product_count DESC, category_id LIMIT 10',
            $page1,
        );

        $page2 = sql_to_paginated_query($baseQuery, limit: 10, offset: 10);
        static::assertSame(
            'SELECT category_id, count(*) AS product_count, avg(price) AS avg_price, min(price) AS min_price, max(price) AS max_price FROM products WHERE active = true GROUP BY category_id HAVING count(*) >= 5 ORDER BY product_count DESC, category_id LIMIT 10 OFFSET 10',
            $page2,
        );
    }

    public function test_iterate_cte_query(): void
    {
        $baseQuery = <<<'SQL'
            WITH RECURSIVE org_tree AS (
                SELECT id, name, manager_id, 1 AS level
                FROM employees
                WHERE manager_id IS NULL

                UNION ALL

                SELECT e.id, e.name, e.manager_id, ot.level + 1
                FROM employees e
                JOIN org_tree ot ON e.manager_id = ot.id
            )
            SELECT id, name, level
            FROM org_tree
            ORDER BY level, name
            SQL;

        $page1 = sql_to_paginated_query($baseQuery, limit: 50, offset: 0);
        static::assertSame(
            'WITH RECURSIVE org_tree AS (SELECT id, name, manager_id, 1 AS level FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.manager_id, ot.level + 1 FROM employees e JOIN org_tree ot ON e.manager_id = ot.id) SELECT id, name, level FROM org_tree ORDER BY level, name LIMIT 50',
            $page1,
        );

        $page2 = sql_to_paginated_query($baseQuery, limit: 50, offset: 50);
        static::assertSame(
            'WITH RECURSIVE org_tree AS (SELECT id, name, manager_id, 1 AS level FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.manager_id, ot.level + 1 FROM employees e JOIN org_tree ot ON e.manager_id = ot.id) SELECT id, name, level FROM org_tree ORDER BY level, name LIMIT 50 OFFSET 50',
            $page2,
        );
    }

    public function test_iterate_through_pages_with_joins(): void
    {
        $baseQuery = <<<'SQL'
            SELECT
                u.id AS user_id,
                u.email,
                o.id AS order_id,
                o.total_amount,
                p.name AS product_name,
                oi.quantity
            FROM users u
            JOIN orders o ON u.id = o.user_id
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON oi.product_id = p.id
            WHERE o.status = 'completed'
            ORDER BY o.created_at DESC, o.id
            SQL;

        $page1 = sql_to_paginated_query($baseQuery, limit: 25, offset: 0);
        static::assertSame(
            'SELECT u.id AS user_id, u.email, o.id AS order_id, o.total_amount, p.name AS product_name, oi.quantity FROM users u JOIN orders o ON u.id = o.user_id JOIN order_items oi ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id WHERE o.status = \'completed\' ORDER BY o.created_at DESC, o.id LIMIT 25',
            $page1,
        );

        $page2 = sql_to_paginated_query($baseQuery, limit: 25, offset: 25);
        static::assertSame(
            'SELECT u.id AS user_id, u.email, o.id AS order_id, o.total_amount, p.name AS product_name, oi.quantity FROM users u JOIN orders o ON u.id = o.user_id JOIN order_items oi ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id WHERE o.status = \'completed\' ORDER BY o.created_at DESC, o.id LIMIT 25 OFFSET 25',
            $page2,
        );

        $page3 = sql_to_paginated_query($baseQuery, limit: 25, offset: 50);
        static::assertSame(
            'SELECT u.id AS user_id, u.email, o.id AS order_id, o.total_amount, p.name AS product_name, oi.quantity FROM users u JOIN orders o ON u.id = o.user_id JOIN order_items oi ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id WHERE o.status = \'completed\' ORDER BY o.created_at DESC, o.id LIMIT 25 OFFSET 50',
            $page3,
        );
    }

    public function test_iterate_union_query(): void
    {
        $baseQuery = <<<'SQL'
            SELECT id, 'order' AS type, message, created_at FROM order_notifications
            UNION ALL
            SELECT id, 'payment' AS type, message, created_at FROM payment_notifications
            UNION ALL
            SELECT id, 'shipping' AS type, message, created_at FROM shipping_notifications
            ORDER BY created_at DESC
            SQL;

        $page1 = sql_to_paginated_query($baseQuery, limit: 20, offset: 0);
        static::assertSame(
            'SELECT * FROM ((SELECT id, \'order\' AS type, message, created_at FROM order_notifications UNION ALL SELECT id, \'payment\' AS type, message, created_at FROM payment_notifications) UNION ALL SELECT id, \'shipping\' AS type, message, created_at FROM shipping_notifications ORDER BY created_at DESC) _pagination_subq LIMIT 20',
            $page1,
        );

        $page2 = sql_to_paginated_query($baseQuery, limit: 20, offset: 20);
        static::assertSame(
            'SELECT * FROM ((SELECT id, \'order\' AS type, message, created_at FROM order_notifications UNION ALL SELECT id, \'payment\' AS type, message, created_at FROM payment_notifications) UNION ALL SELECT id, \'shipping\' AS type, message, created_at FROM shipping_notifications ORDER BY created_at DESC) _pagination_subq LIMIT 20 OFFSET 20',
            $page2,
        );
    }

    public function test_nested_cte_pagination(): void
    {
        $baseQuery = <<<'SQL'
            WITH
                active_users AS (
                    SELECT id, name, email
                    FROM users
                    WHERE status = 'active'
                ),
                user_orders AS (
                    SELECT
                        au.id AS user_id,
                        au.name,
                        COUNT(o.id) AS order_count,
                        SUM(o.total_amount) AS total_spent
                    FROM active_users au
                    LEFT JOIN orders o ON au.id = o.user_id
                    GROUP BY au.id, au.name
                )
            SELECT user_id, name, order_count, total_spent
            FROM user_orders
            WHERE order_count > 0
            ORDER BY total_spent DESC
            SQL;

        $page1 = sql_to_paginated_query($baseQuery, limit: 15, offset: 0);
        static::assertSame(
            'WITH active_users AS (SELECT id, name, email FROM users WHERE status = \'active\'), user_orders AS (SELECT au.id AS user_id, au.name, count(o.id) AS order_count, sum(o.total_amount) AS total_spent FROM active_users au LEFT JOIN orders o ON au.id = o.user_id GROUP BY au.id, au.name) SELECT user_id, name, order_count, total_spent FROM user_orders WHERE order_count > 0 ORDER BY total_spent DESC LIMIT 15',
            $page1,
        );

        $page2 = sql_to_paginated_query($baseQuery, limit: 15, offset: 15);
        static::assertSame(
            'WITH active_users AS (SELECT id, name, email FROM users WHERE status = \'active\'), user_orders AS (SELECT au.id AS user_id, au.name, count(o.id) AS order_count, sum(o.total_amount) AS total_spent FROM active_users au LEFT JOIN orders o ON au.id = o.user_id GROUP BY au.id, au.name) SELECT user_id, name, order_count, total_spent FROM user_orders WHERE order_count > 0 ORDER BY total_spent DESC LIMIT 15 OFFSET 15',
            $page2,
        );
    }

    public function test_pagination_with_lateral_subquery(): void
    {
        $baseQuery = <<<'SQL'
            SELECT
                u.id,
                u.name,
                recent_orders.order_id,
                recent_orders.amount
            FROM users u,
            LATERAL (
                SELECT o.id AS order_id, o.total_amount AS amount
                FROM orders o
                WHERE o.user_id = u.id
                ORDER BY o.created_at DESC
                LIMIT 3
            ) AS recent_orders
            ORDER BY u.id
            SQL;

        $page1 = sql_to_paginated_query($baseQuery, limit: 10, offset: 0);
        static::assertSame(
            'SELECT u.id, u.name, recent_orders.order_id, recent_orders.amount FROM users u, LATERAL (SELECT o.id AS order_id, o.total_amount AS amount FROM orders o WHERE o.user_id = u.id ORDER BY o.created_at DESC LIMIT 3) recent_orders ORDER BY u.id LIMIT 10',
            $page1,
        );

        $page2 = sql_to_paginated_query($baseQuery, limit: 10, offset: 10);
        static::assertSame(
            'SELECT u.id, u.name, recent_orders.order_id, recent_orders.amount FROM users u, LATERAL (SELECT o.id AS order_id, o.total_amount AS amount FROM orders o WHERE o.user_id = u.id ORDER BY o.created_at DESC LIMIT 3) recent_orders ORDER BY u.id LIMIT 10 OFFSET 10',
            $page2,
        );
    }

    public function test_product_catalog_with_filters(): void
    {
        $baseQuery = <<<'SQL'
            SELECT
                p.id,
                p.name,
                p.price,
                c.name AS category_name,
                b.name AS brand_name,
                COALESCE(r.avg_rating, 0) AS rating
            FROM products p
            JOIN categories c ON p.category_id = c.id
            JOIN brands b ON p.brand_id = b.id
            LEFT JOIN (
                SELECT product_id, AVG(rating) AS avg_rating
                FROM reviews
                GROUP BY product_id
            ) r ON p.id = r.product_id
            WHERE p.active = true
                AND p.price BETWEEN 10 AND 100
                AND c.slug IN ('electronics', 'computers')
                AND p.stock_quantity > 0
            ORDER BY r.avg_rating DESC NULLS LAST, p.name
            SQL;

        $page1 = sql_to_paginated_query($baseQuery, limit: 24, offset: 0);
        static::assertSame(
            'SELECT p.id, p.name, p.price, c.name AS category_name, b.name AS brand_name, COALESCE(r.avg_rating, 0) AS rating FROM products p JOIN categories c ON p.category_id = c.id JOIN brands b ON p.brand_id = b.id LEFT JOIN (SELECT product_id, avg(rating) AS avg_rating FROM reviews GROUP BY product_id) r ON p.id = r.product_id WHERE p.active = true AND p.price BETWEEN 10 AND 100 AND c.slug IN (\'electronics\', \'computers\') AND p.stock_quantity > 0 ORDER BY r.avg_rating DESC NULLS LAST, p.name LIMIT 24',
            $page1,
        );

        $page2 = sql_to_paginated_query($baseQuery, limit: 24, offset: 24);
        static::assertSame(
            'SELECT p.id, p.name, p.price, c.name AS category_name, b.name AS brand_name, COALESCE(r.avg_rating, 0) AS rating FROM products p JOIN categories c ON p.category_id = c.id JOIN brands b ON p.brand_id = b.id LEFT JOIN (SELECT product_id, avg(rating) AS avg_rating FROM reviews GROUP BY product_id) r ON p.id = r.product_id WHERE p.active = true AND p.price BETWEEN 10 AND 100 AND c.slug IN (\'electronics\', \'computers\') AND p.stock_quantity > 0 ORDER BY r.avg_rating DESC NULLS LAST, p.name LIMIT 24 OFFSET 24',
            $page2,
        );

        $page3 = sql_to_paginated_query($baseQuery, limit: 24, offset: 48);
        static::assertSame(
            'SELECT p.id, p.name, p.price, c.name AS category_name, b.name AS brand_name, COALESCE(r.avg_rating, 0) AS rating FROM products p JOIN categories c ON p.category_id = c.id JOIN brands b ON p.brand_id = b.id LEFT JOIN (SELECT product_id, avg(rating) AS avg_rating FROM reviews GROUP BY product_id) r ON p.id = r.product_id WHERE p.active = true AND p.price BETWEEN 10 AND 100 AND c.slug IN (\'electronics\', \'computers\') AND p.stock_quantity > 0 ORDER BY r.avg_rating DESC NULLS LAST, p.name LIMIT 24 OFFSET 48',
            $page3,
        );
    }
}
