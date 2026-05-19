<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use Flow\PostgreSql\AST\Transformers\SortOrder;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\sql_keyset_column;
use function Flow\PostgreSql\DSL\sql_to_keyset_query;

final class KeysetPaginationEdgeCasesTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_audit_log_iteration(): void
    {
        $baseQuery = <<<'SQL'
            SELECT
                al.id,
                al.event_type,
                al.entity_type,
                al.entity_id,
                al.user_id,
                al.ip_address,
                al.changes,
                al.created_at
            FROM audit_log al
            WHERE al.event_type IN ('create', 'update', 'delete')
                AND al.created_at >= '2025-01-01'
            ORDER BY al.created_at DESC, al.id DESC
            SQL;

        $columns = [
            sql_keyset_column('al.created_at', SortOrder::DESC),
            sql_keyset_column('al.id', SortOrder::DESC),
        ];

        $page1 = sql_to_keyset_query($baseQuery, limit: 1000, columns: $columns, cursor: null);
        static::assertSame(
            'SELECT al.id, al.event_type, al.entity_type, al.entity_id, al.user_id, al.ip_address, al.changes, al.created_at FROM audit_log al WHERE al.event_type IN (\'create\', \'update\', \'delete\') AND al.created_at >= \'2025-01-01\' ORDER BY al.created_at DESC, al.id DESC LIMIT 1000',
            $page1,
        );

        $page100 = sql_to_keyset_query(
            $baseQuery,
            limit: 1000,
            columns: $columns,
            cursor: ['2025-01-05 08:30:00', 50000],
        );
        static::assertSame(
            'SELECT al.id, al.event_type, al.entity_type, al.entity_id, al.user_id, al.ip_address, al.changes, al.created_at FROM audit_log al WHERE (al.event_type IN (\'create\', \'update\', \'delete\') AND al.created_at >= \'2025-01-01\') AND (al.created_at < $1 OR (al.created_at = $1 AND al.id < $2)) ORDER BY al.created_at DESC, al.id DESC LIMIT 1000',
            $page100,
        );
    }

    public function test_iterate_through_pages(): void
    {
        $baseQuery = <<<'SQL'
            SELECT
                a.id,
                a.user_id,
                a.action_type,
                a.metadata,
                a.created_at
            FROM activity_log a
            ORDER BY a.created_at DESC, a.id DESC
            SQL;

        $columns = [
            sql_keyset_column('a.created_at', SortOrder::DESC),
            sql_keyset_column('a.id', SortOrder::DESC),
        ];

        $page1 = sql_to_keyset_query($baseQuery, limit: 50, columns: $columns, cursor: null);
        static::assertSame(
            'SELECT a.id, a.user_id, a.action_type, a.metadata, a.created_at FROM activity_log a ORDER BY a.created_at DESC, a.id DESC LIMIT 50',
            $page1,
        );

        $page2 = sql_to_keyset_query($baseQuery, limit: 50, columns: $columns, cursor: ['2025-01-15 14:30:00', 1000]);
        static::assertSame(
            'SELECT a.id, a.user_id, a.action_type, a.metadata, a.created_at FROM activity_log a WHERE a.created_at < $1 OR (a.created_at = $1 AND a.id < $2) ORDER BY a.created_at DESC, a.id DESC LIMIT 50',
            $page2,
        );

        $page3 = sql_to_keyset_query($baseQuery, limit: 50, columns: $columns, cursor: ['2025-01-15 12:15:00', 850]);
        static::assertSame(
            'SELECT a.id, a.user_id, a.action_type, a.metadata, a.created_at FROM activity_log a WHERE a.created_at < $1 OR (a.created_at = $1 AND a.id < $2) ORDER BY a.created_at DESC, a.id DESC LIMIT 50',
            $page3,
        );
    }

    public function test_with_complex_joins(): void
    {
        $baseQuery = <<<'SQL'
            SELECT
                o.id AS order_id,
                o.created_at,
                o.total_amount,
                u.email AS customer_email,
                p.name AS product_name
            FROM orders o
            JOIN users u ON o.user_id = u.id
            JOIN order_items oi ON o.id = oi.order_id
            JOIN products p ON oi.product_id = p.id
            WHERE o.status = 'completed'
            ORDER BY o.created_at DESC, o.id DESC
            SQL;

        $columns = [
            sql_keyset_column('o.created_at', SortOrder::DESC),
            sql_keyset_column('o.id', SortOrder::DESC),
        ];

        $page1 = sql_to_keyset_query($baseQuery, limit: 25, columns: $columns, cursor: null);
        static::assertSame(
            'SELECT o.id AS order_id, o.created_at, o.total_amount, u.email AS customer_email, p.name AS product_name FROM orders o JOIN users u ON o.user_id = u.id JOIN order_items oi ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id WHERE o.status = \'completed\' ORDER BY o.created_at DESC, o.id DESC LIMIT 25',
            $page1,
        );

        $page2 = sql_to_keyset_query($baseQuery, limit: 25, columns: $columns, cursor: ['2025-01-10 09:45:00', 5000]);
        static::assertSame(
            'SELECT o.id AS order_id, o.created_at, o.total_amount, u.email AS customer_email, p.name AS product_name FROM orders o JOIN users u ON o.user_id = u.id JOIN order_items oi ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id WHERE o.status = \'completed\' AND (o.created_at < $1 OR (o.created_at = $1 AND o.id < $2)) ORDER BY o.created_at DESC, o.id DESC LIMIT 25',
            $page2,
        );
    }

    public function test_with_existing_complex_where(): void
    {
        $baseQuery = <<<'SQL'
            SELECT
                p.id,
                p.name,
                p.price,
                p.category_id,
                p.created_at
            FROM products p
            WHERE p.active = true
                AND p.price BETWEEN 50 AND 500
                AND p.category_id IN (1, 2, 3, 4, 5)
                AND p.stock_quantity > 0
                AND (p.name ILIKE '%phone%' OR p.name ILIKE '%tablet%')
            ORDER BY p.created_at DESC, p.id DESC
            SQL;

        $columns = [
            sql_keyset_column('p.created_at', SortOrder::DESC),
            sql_keyset_column('p.id', SortOrder::DESC),
        ];

        $page1 = sql_to_keyset_query($baseQuery, limit: 12, columns: $columns, cursor: null);
        static::assertSame(
            'SELECT p.id, p.name, p.price, p.category_id, p.created_at FROM products p WHERE p.active = true AND p.price BETWEEN 50 AND 500 AND p.category_id IN (1, 2, 3, 4, 5) AND p.stock_quantity > 0 AND (p.name ILIKE \'%phone%\' OR p.name ILIKE \'%tablet%\') ORDER BY p.created_at DESC, p.id DESC LIMIT 12',
            $page1,
        );

        $page2 = sql_to_keyset_query($baseQuery, limit: 12, columns: $columns, cursor: ['2025-01-10 15:00:00', 750]);
        static::assertSame(
            'SELECT p.id, p.name, p.price, p.category_id, p.created_at FROM products p WHERE (p.active = true AND p.price BETWEEN 50 AND 500 AND p.category_id IN (1, 2, 3, 4, 5) AND p.stock_quantity > 0 AND (p.name ILIKE \'%phone%\' OR p.name ILIKE \'%tablet%\')) AND (p.created_at < $1 OR (p.created_at = $1 AND p.id < $2)) ORDER BY p.created_at DESC, p.id DESC LIMIT 12',
            $page2,
        );
    }

    public function test_with_mixed_sort_orders(): void
    {
        $baseQuery = <<<'SQL'
            SELECT
                t.id,
                t.title,
                t.priority,
                t.created_at,
                t.status
            FROM tickets t
            WHERE t.status != 'closed'
            ORDER BY t.priority DESC, t.created_at ASC, t.id ASC
            SQL;

        $columns = [
            sql_keyset_column('t.priority', SortOrder::DESC),
            sql_keyset_column('t.created_at', SortOrder::ASC),
            sql_keyset_column('t.id', SortOrder::ASC),
        ];

        $page1 = sql_to_keyset_query($baseQuery, limit: 20, columns: $columns, cursor: null);
        static::assertSame(
            'SELECT t.id, t.title, t.priority, t.created_at, t.status FROM tickets t WHERE t.status <> \'closed\' ORDER BY t.priority DESC, t.created_at ASC, t.id ASC LIMIT 20',
            $page1,
        );

        $page2 = sql_to_keyset_query($baseQuery, limit: 20, columns: $columns, cursor: [5, '2025-01-01 10:00:00', 100]);
        static::assertSame(
            'SELECT t.id, t.title, t.priority, t.created_at, t.status FROM tickets t WHERE t.status <> \'closed\' AND (t.priority < $1 OR (t.priority = $1 AND t.created_at > $2) OR (t.priority = $1 AND t.created_at = $2 AND t.id > $3)) ORDER BY t.priority DESC, t.created_at ASC, t.id ASC LIMIT 20',
            $page2,
        );
    }

    public function test_with_window_functions(): void
    {
        $baseQuery = <<<'SQL'
            SELECT
                e.id,
                e.name,
                e.department_id,
                e.salary,
                RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS salary_rank,
                e.hire_date
            FROM employees e
            WHERE e.active = true
            ORDER BY e.hire_date DESC, e.id DESC
            SQL;

        $columns = [
            sql_keyset_column('e.hire_date', SortOrder::DESC),
            sql_keyset_column('e.id', SortOrder::DESC),
        ];

        $page1 = sql_to_keyset_query($baseQuery, limit: 30, columns: $columns, cursor: null);
        static::assertSame(
            'SELECT e.id, e.name, e.department_id, e.salary, rank() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS salary_rank, e.hire_date FROM employees e WHERE e.active = true ORDER BY e.hire_date DESC, e.id DESC LIMIT 30',
            $page1,
        );

        $page2 = sql_to_keyset_query($baseQuery, limit: 30, columns: $columns, cursor: ['2024-06-15', 500]);
        static::assertSame(
            'SELECT e.id, e.name, e.department_id, e.salary, rank() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS salary_rank, e.hire_date FROM employees e WHERE e.active = true AND (e.hire_date < $1 OR (e.hire_date = $1 AND e.id < $2)) ORDER BY e.hire_date DESC, e.id DESC LIMIT 30',
            $page2,
        );
    }
}
