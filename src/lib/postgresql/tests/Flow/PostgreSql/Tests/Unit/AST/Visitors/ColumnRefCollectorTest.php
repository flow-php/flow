<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Visitors;

use Flow\PostgreSql\AST\Traverser;
use Flow\PostgreSql\AST\Visitors\ColumnRefCollector;
use Flow\PostgreSql\Protobuf\AST\{ColumnRef, ParseResult};
use PHPUnit\Framework\TestCase;

final class ColumnRefCollectorTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_collects_column_from_order_by() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT id FROM users ORDER BY name'));

        self::assertCount(2, $collector->getColumnRefs());
    }

    public function test_collects_column_from_where_clause() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT 1 FROM users WHERE active = true'));

        self::assertCount(1, $collector->getColumnRefs());
    }

    public function test_collects_columns_from_join_condition() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM users u JOIN orders o ON u.id = o.user_id'));

        self::assertCount(3, $collector->getColumnRefs());
    }

    public function test_collects_columns_from_select() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT id, name, email FROM users'));

        self::assertCount(3, $collector->getColumnRefs());
    }

    public function test_collects_columns_from_subquery() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM (SELECT id, name FROM users) sub'));

        self::assertCount(3, $collector->getColumnRefs());
    }

    public function test_collects_table_qualified_columns() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT u.id, u.name FROM users u'));

        self::assertCount(2, $collector->getColumnRefs());

        foreach ($collector->getColumnRefs() as $columnRef) {
            self::assertCount(2, $columnRef->getFields());
        }
    }

    public function test_enter_returns_null() : void
    {
        $collector = new ColumnRefCollector();
        $columnRef = new ColumnRef();

        self::assertNull($collector->enter($columnRef));
    }

    public function test_get_column_refs_returns_empty_array_initially() : void
    {
        $collector = new ColumnRefCollector();

        self::assertSame([], $collector->getColumnRefs());
    }

    public function test_leave_returns_null() : void
    {
        $collector = new ColumnRefCollector();
        $columnRef = new ColumnRef();

        self::assertNull($collector->leave($columnRef));
    }

    public function test_node_classes_returns_column_ref_class() : void
    {
        self::assertSame([ColumnRef::class], ColumnRefCollector::nodeClasses());
    }

    public function test_reset_clears_collected_column_refs() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT id, name FROM users'));

        self::assertCount(2, $collector->getColumnRefs());

        $collector->reset();

        self::assertSame([], $collector->getColumnRefs());
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
