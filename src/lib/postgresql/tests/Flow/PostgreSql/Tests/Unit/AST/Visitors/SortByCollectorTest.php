<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Visitors;

use Flow\PostgreSql\AST\Traverser;
use Flow\PostgreSql\AST\Visitors\SortByCollector;
use Flow\PostgreSql\Protobuf\AST\{ParseResult, SortBy};
use PHPUnit\Framework\TestCase;

final class SortByCollectorTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_collects_multiple_sort_by_columns() : void
    {
        $collector = new SortByCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT id FROM users ORDER BY name, created_at'));

        self::assertCount(2, $collector->getSortByClauses());
    }

    public function test_collects_sort_by_from_order_by_clause() : void
    {
        $collector = new SortByCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT id FROM users ORDER BY name'));

        self::assertCount(1, $collector->getSortByClauses());
    }

    public function test_collects_sort_by_from_subquery() : void
    {
        $collector = new SortByCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM (SELECT id, name FROM users ORDER BY id) sub'));

        self::assertCount(1, $collector->getSortByClauses());
    }

    public function test_collects_sort_by_with_direction() : void
    {
        $collector = new SortByCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT id FROM users ORDER BY name ASC, created_at DESC'));

        self::assertCount(2, $collector->getSortByClauses());
    }

    public function test_enter_returns_null() : void
    {
        $collector = new SortByCollector();
        $sortBy = new SortBy();

        self::assertNull($collector->enter($sortBy));
    }

    public function test_get_sort_by_clauses_returns_empty_array_initially() : void
    {
        $collector = new SortByCollector();

        self::assertSame([], $collector->getSortByClauses());
    }

    public function test_has_sort_by_returns_false_when_no_order_by() : void
    {
        $collector = new SortByCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM users'));

        self::assertFalse($collector->hasSortBy());
    }

    public function test_has_sort_by_returns_true_when_order_by_exists() : void
    {
        $collector = new SortByCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT id FROM users ORDER BY name'));

        self::assertTrue($collector->hasSortBy());
    }

    public function test_leave_returns_null() : void
    {
        $collector = new SortByCollector();
        $sortBy = new SortBy();

        self::assertNull($collector->leave($sortBy));
    }

    public function test_node_classes_returns_sort_by_class() : void
    {
        self::assertSame([SortBy::class], SortByCollector::nodeClasses());
    }

    public function test_reset_clears_collected_sort_by_clauses() : void
    {
        $collector = new SortByCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT id FROM users ORDER BY name, created_at'));

        self::assertCount(2, $collector->getSortByClauses());

        $collector->reset();

        self::assertSame([], $collector->getSortByClauses());
    }

    public function test_returns_empty_for_query_without_order_by() : void
    {
        $collector = new SortByCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM users'));

        self::assertSame([], $collector->getSortByClauses());
    }

    private function parseQuery(string $sql) : ParseResult
    {
        /** @var string $json */
        $json = \pg_query_parse($sql);
        $result = new ParseResult();
        $result->mergeFromJsonString($json);

        return $result;
    }
}
