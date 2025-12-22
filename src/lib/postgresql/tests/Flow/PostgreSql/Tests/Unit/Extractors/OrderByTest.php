<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Extractors;

use function Flow\PostgreSql\DSL\sql_parse;
use Flow\PostgreSql\Extractors\OrderBy;
use Flow\PostgreSql\QueryBuilder\Clause\SortDirection;
use PHPUnit\Framework\TestCase;

final class OrderByTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_all_returns_empty_for_query_without_order_by() : void
    {
        $orderBy = new OrderBy(sql_parse('SELECT * FROM users'));

        self::assertSame([], $orderBy->all());
    }

    public function test_all_returns_multiple_order_by_items() : void
    {
        $orderBy = new OrderBy(sql_parse('SELECT id FROM users ORDER BY name, created_at'));

        $items = $orderBy->all();

        self::assertCount(2, $items);
        self::assertSame('name', $items[0]->column());
        self::assertSame('created_at', $items[1]->column());
    }

    public function test_all_returns_order_by_items() : void
    {
        $orderBy = new OrderBy(sql_parse('SELECT id, name FROM users ORDER BY name'));

        $items = $orderBy->all();

        self::assertCount(1, $items);
        self::assertSame('name', $items[0]->column());
        self::assertSame(SortDirection::DEFAULT, $items[0]->direction());
    }

    public function test_all_returns_order_by_with_direction() : void
    {
        $orderBy = new OrderBy(sql_parse('SELECT id FROM users ORDER BY name ASC, created_at DESC'));

        $items = $orderBy->all();

        self::assertCount(2, $items);
        self::assertSame('name', $items[0]->column());
        self::assertSame(SortDirection::ASC, $items[0]->direction());
        self::assertSame('created_at', $items[1]->column());
        self::assertSame(SortDirection::DESC, $items[1]->direction());
    }

    public function test_has_order_by_returns_false_when_no_order_by() : void
    {
        $orderBy = new OrderBy(sql_parse('SELECT * FROM users'));

        self::assertFalse($orderBy->hasOrderBy());
    }

    public function test_has_order_by_returns_true_for_subquery_with_order_by() : void
    {
        $orderBy = new OrderBy(sql_parse('SELECT * FROM (SELECT id FROM users ORDER BY id) sub'));

        self::assertTrue($orderBy->hasOrderBy());
    }

    public function test_has_order_by_returns_true_when_order_by_exists() : void
    {
        $orderBy = new OrderBy(sql_parse('SELECT id FROM users ORDER BY name'));

        self::assertTrue($orderBy->hasOrderBy());
    }

    public function test_order_by_item_returns_null_column_for_expression() : void
    {
        $orderBy = new OrderBy(sql_parse('SELECT id FROM users ORDER BY UPPER(name)'));

        $items = $orderBy->all();

        self::assertCount(1, $items);
        self::assertNull($items[0]->column());
    }
}
