<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Visitors;

use Flow\PostgreSql\AST\Traverser;
use Flow\PostgreSql\AST\Visitors\SelectStmtDepthCollector;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use PHPUnit\Framework\TestCase;

final class SelectStmtDepthCollectorTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_depth_for_cte_query(): void
    {
        $collector = new SelectStmtDepthCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('WITH cte AS (SELECT * FROM t) SELECT * FROM cte'));

        static::assertSame(2, $collector->getMaxDepth());
    }

    public function test_depth_for_four_nested_selects(): void
    {
        $collector = new SelectStmtDepthCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery(
            'SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM t) AS a) AS b) AS c',
        ));

        static::assertSame(4, $collector->getMaxDepth());
    }

    public function test_depth_for_simple_select(): void
    {
        $collector = new SelectStmtDepthCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM users'));

        static::assertSame(1, $collector->getMaxDepth());
    }

    public function test_depth_for_subquery_in_from(): void
    {
        $collector = new SelectStmtDepthCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM (SELECT * FROM t) AS sub'));

        static::assertSame(2, $collector->getMaxDepth());
    }

    public function test_depth_for_subquery_in_where(): void
    {
        $collector = new SelectStmtDepthCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)'));

        static::assertSame(2, $collector->getMaxDepth());
    }

    public function test_depth_for_three_nested_selects(): void
    {
        $collector = new SelectStmtDepthCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM (SELECT * FROM (SELECT * FROM t) AS a) AS b'));

        static::assertSame(3, $collector->getMaxDepth());
    }

    public function test_depth_for_union_query(): void
    {
        $collector = new SelectStmtDepthCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM a UNION SELECT * FROM b'));

        static::assertSame(2, $collector->getMaxDepth());
    }

    public function test_enter_returns_null(): void
    {
        $collector = new SelectStmtDepthCollector();
        $selectStmt = new SelectStmt();

        static::assertNull($collector->enter($selectStmt));
    }

    public function test_get_max_depth_returns_zero_initially(): void
    {
        $collector = new SelectStmtDepthCollector();

        static::assertSame(0, $collector->getMaxDepth());
    }

    public function test_leave_returns_null(): void
    {
        $collector = new SelectStmtDepthCollector();
        $selectStmt = new SelectStmt();

        static::assertNull($collector->leave($selectStmt));
    }

    public function test_node_classes_returns_select_stmt_class(): void
    {
        static::assertSame([SelectStmt::class], SelectStmtDepthCollector::nodeClasses());
    }

    public function test_reset_clears_depth(): void
    {
        $collector = new SelectStmtDepthCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM (SELECT * FROM t) AS sub'));

        static::assertSame(2, $collector->getMaxDepth());

        $collector->reset();

        static::assertSame(0, $collector->getMaxDepth());
    }

    private function parseQuery(string $sql): ParseResult
    {
        /** @var string $json */
        $json = \pg_query_parse($sql);
        $result = new ParseResult();
        $result->mergeFromJsonString($json);

        return $result;
    }
}
