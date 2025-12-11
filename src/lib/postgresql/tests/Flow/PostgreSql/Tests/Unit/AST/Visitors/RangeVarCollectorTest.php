<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Visitors;

use Flow\PostgreSql\AST\Traverser;
use Flow\PostgreSql\AST\Visitors\RangeVarCollector;
use Flow\PostgreSql\Protobuf\AST\{ParseResult, RangeVar};
use PHPUnit\Framework\TestCase;

final class RangeVarCollectorTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_collects_table_from_cte() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active'));

        self::assertCount(2, $collector->getRangeVars());

        $tableNames = \array_map(fn (RangeVar $rv) => $rv->getRelname(), $collector->getRangeVars());
        self::assertContains('users', $tableNames);
        self::assertContains('active', $tableNames);
    }

    public function test_collects_table_from_delete() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('DELETE FROM users WHERE id = 1'));

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_collects_table_from_insert() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('INSERT INTO users (name) VALUES (\'john\')'));

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_collects_table_from_select() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM users'));

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_collects_table_from_subquery() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM (SELECT * FROM users) sub'));

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_collects_table_from_update() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('UPDATE users SET name = \'john\' WHERE id = 1'));

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_collects_table_with_alias() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM users AS u'));

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
        self::assertNotNull($collector->getRangeVars()[0]->getAlias());
        self::assertSame('u', $collector->getRangeVars()[0]->getAlias()->getAliasname());
    }

    public function test_collects_table_with_schema() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM public.users'));

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
        self::assertSame('public', $collector->getRangeVars()[0]->getSchemaname());
    }

    public function test_collects_tables_from_join() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM users u JOIN orders o ON u.id = o.user_id'));

        self::assertCount(2, $collector->getRangeVars());

        $tableNames = \array_map(fn (RangeVar $rv) => $rv->getRelname(), $collector->getRangeVars());
        self::assertContains('users', $tableNames);
        self::assertContains('orders', $tableNames);
    }

    public function test_enter_returns_null() : void
    {
        $collector = new RangeVarCollector();
        $rangeVar = new RangeVar();

        self::assertNull($collector->enter($rangeVar));
    }

    public function test_get_range_vars_returns_empty_array_initially() : void
    {
        $collector = new RangeVarCollector();

        self::assertSame([], $collector->getRangeVars());
    }

    public function test_leave_returns_null() : void
    {
        $collector = new RangeVarCollector();
        $rangeVar = new RangeVar();

        self::assertNull($collector->leave($rangeVar));
    }

    public function test_node_class_returns_range_var_class() : void
    {
        self::assertSame(RangeVar::class, RangeVarCollector::nodeClass());
    }

    public function test_reset_clears_collected_range_vars() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM users u JOIN orders o ON u.id = o.user_id'));

        self::assertCount(2, $collector->getRangeVars());

        $collector->reset();

        self::assertSame([], $collector->getRangeVars());
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
