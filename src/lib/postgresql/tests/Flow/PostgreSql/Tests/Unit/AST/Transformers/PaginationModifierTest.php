<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use function Flow\PostgreSql\DSL\sql_parse;
use Flow\PostgreSql\AST\Transformers\{PaginationConfig, PaginationModifier};
use Flow\PostgreSql\Exception\PaginationException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PaginationModifierTest extends TestCase
{
    public static function paginationProvider() : \Generator
    {
        yield 'simple select with order by - limit only' => [
            'SELECT * FROM users ORDER BY id',
            new PaginationConfig(10),
            'SELECT * FROM users ORDER BY id LIMIT 10',
        ];

        yield 'simple select with order by - limit and offset' => [
            'SELECT * FROM users ORDER BY id',
            new PaginationConfig(10, 5),
            'SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 5',
        ];

        yield 'select with where clause - limit only' => [
            'SELECT id, name FROM users WHERE active = true',
            new PaginationConfig(20),
            'SELECT id, name FROM users WHERE active = true LIMIT 20',
        ];

        yield 'order by name asc - limit only' => [
            'SELECT * FROM users ORDER BY name ASC',
            new PaginationConfig(10),
            'SELECT * FROM users ORDER BY name ASC LIMIT 10',
        ];

        yield 'order by name asc - limit and offset' => [
            'SELECT * FROM users ORDER BY name ASC',
            new PaginationConfig(10, 20),
            'SELECT * FROM users ORDER BY name ASC LIMIT 10 OFFSET 20',
        ];

        yield 'distinct query' => [
            'SELECT DISTINCT name FROM users',
            new PaginationConfig(10),
            'SELECT DISTINCT name FROM users LIMIT 10',
        ];

        yield 'group by having' => [
            'SELECT dept, COUNT(*) as cnt FROM users GROUP BY dept HAVING COUNT(*) > 5',
            new PaginationConfig(10),
            'SELECT dept, count(*) AS cnt FROM users GROUP BY dept HAVING count(*) > 5 LIMIT 10',
        ];

        yield 'inner join' => [
            'SELECT u.id, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id',
            new PaginationConfig(10),
            'SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.user_id LIMIT 10',
        ];

        yield 'left join' => [
            'SELECT u.id, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id',
            new PaginationConfig(10),
            'SELECT u.id, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id LIMIT 10',
        ];

        yield 'multiple joins' => [
            'SELECT u.id, o.amount, p.name FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id',
            new PaginationConfig(10),
            'SELECT u.id, o.amount, p.name FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id LIMIT 10',
        ];

        yield 'self join' => [
            'SELECT e.name AS employee, m.name AS manager FROM employees e LEFT JOIN employees m ON e.manager_id = m.id',
            new PaginationConfig(10),
            'SELECT e.name AS employee, m.name AS manager FROM employees e LEFT JOIN employees m ON e.manager_id = m.id LIMIT 10',
        ];

        yield 'lateral join' => [
            'SELECT u.id, latest.amount FROM users u, LATERAL (SELECT amount FROM orders WHERE user_id = u.id ORDER BY created_at DESC LIMIT 1) AS latest',
            new PaginationConfig(10),
            'SELECT u.id, latest.amount FROM users u, LATERAL (SELECT amount FROM orders WHERE user_id = u.id ORDER BY created_at DESC LIMIT 1) latest LIMIT 10',
        ];

        yield 'subquery in from clause' => [
            'SELECT * FROM (SELECT id FROM users) AS sub',
            new PaginationConfig(10),
            'SELECT * FROM (SELECT id FROM users) sub LIMIT 10',
        ];

        yield 'subquery in where clause' => [
            'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)',
            new PaginationConfig(10),
            'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) LIMIT 10',
        ];

        yield 'deeply nested subqueries' => [
            'SELECT * FROM (SELECT * FROM (SELECT id FROM users) AS inner1) AS outer1',
            new PaginationConfig(10),
            'SELECT * FROM (SELECT * FROM (SELECT id FROM users) inner1) outer1 LIMIT 10',
        ];

        yield 'cte main query gets paginated' => [
            'WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active',
            new PaginationConfig(10),
            'WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active LIMIT 10',
        ];

        yield 'window function' => [
            'SELECT id, name, ROW_NUMBER() OVER (ORDER BY created_at) as rn FROM users',
            new PaginationConfig(10),
            'SELECT id, name, row_number() OVER (ORDER BY created_at) AS rn FROM users LIMIT 10',
        ];

        yield 'window function with partition' => [
            'SELECT id, dept, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rank FROM employees',
            new PaginationConfig(10),
            'SELECT id, dept, rank() OVER (PARTITION BY dept ORDER BY salary DESC) AS rank FROM employees LIMIT 10',
        ];

        yield 'values clause' => [
            'SELECT * FROM (VALUES (1, \'a\'), (2, \'b\'), (3, \'c\')) AS t(id, name)',
            new PaginationConfig(2),
            'SELECT * FROM (VALUES (1, \'a\'), (2, \'b\'), (3, \'c\')) t(id, name) LIMIT 2',
        ];

        yield 'overrides existing limit' => [
            'SELECT * FROM users LIMIT 100',
            new PaginationConfig(10),
            'SELECT * FROM users LIMIT 10',
        ];

        yield 'overrides existing limit and offset' => [
            'SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 50',
            new PaginationConfig(10, 5),
            'SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 5',
        ];

        yield 'clears existing offset when new offset is zero' => [
            'SELECT * FROM users LIMIT 100 OFFSET 50',
            new PaginationConfig(10, 0),
            'SELECT * FROM users LIMIT 10',
        ];

        yield 'union query wraps in subquery' => [
            'SELECT id FROM users UNION SELECT id FROM admins',
            new PaginationConfig(10),
            'SELECT * FROM (SELECT id FROM users UNION SELECT id FROM admins) _pagination_subq LIMIT 10',
        ];

        yield 'union all wraps in subquery' => [
            'SELECT id FROM users UNION ALL SELECT id FROM admins',
            new PaginationConfig(10),
            'SELECT * FROM (SELECT id FROM users UNION ALL SELECT id FROM admins) _pagination_subq LIMIT 10',
        ];

        yield 'intersect query wraps in subquery' => [
            'SELECT id FROM users INTERSECT SELECT id FROM admins ORDER BY id',
            new PaginationConfig(5, 2),
            'SELECT * FROM (SELECT id FROM users INTERSECT SELECT id FROM admins ORDER BY id) _pagination_subq LIMIT 5 OFFSET 2',
        ];

        yield 'except query wraps in subquery' => [
            'SELECT id FROM users EXCEPT SELECT id FROM banned',
            new PaginationConfig(15),
            'SELECT * FROM (SELECT id FROM users EXCEPT SELECT id FROM banned) _pagination_subq LIMIT 15',
        ];

        yield 'nested union wraps in subquery' => [
            '(SELECT id FROM users UNION SELECT id FROM admins) UNION SELECT id FROM guests',
            new PaginationConfig(10),
            'SELECT * FROM ((SELECT id FROM users UNION SELECT id FROM admins) UNION SELECT id FROM guests) _pagination_subq LIMIT 10',
        ];
    }

    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_offset_without_order_by_throws_exception() : void
    {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage('OFFSET without ORDER BY produces non-deterministic results');

        $parsed = sql_parse('SELECT * FROM users');
        $parsed->traverse(new PaginationModifier(new PaginationConfig(10, 5)));
    }

    #[DataProvider('paginationProvider')]
    public function test_pagination(string $inputSql, PaginationConfig $config, string $expectedSql) : void
    {
        $parsed = sql_parse($inputSql);
        $parsed->traverse(new PaginationModifier($config));

        self::assertSame($expectedSql, $parsed->deparse());
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
}
