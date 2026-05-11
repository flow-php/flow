<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Visitors;

use Flow\PostgreSql\AST\Traverser;
use Flow\PostgreSql\AST\Visitors\FuncCallCollector;
use Flow\PostgreSql\Protobuf\AST\FuncCall;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use PHPUnit\Framework\TestCase;

final class FuncCallCollectorTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_collects_aggregate_functions(): void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT COUNT(*), SUM(amount), AVG(price) FROM orders'));

        static::assertCount(3, $collector->getFuncCalls());
    }

    public function test_collects_functions_from_select(): void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT now()'));

        static::assertCount(1, $collector->getFuncCalls());
    }

    public function test_collects_functions_from_where_clause(): void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT * FROM users WHERE created_at > now()'));

        static::assertCount(1, $collector->getFuncCalls());
    }

    public function test_collects_nested_functions(): void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT UPPER(TRIM(LOWER(name))) FROM users'));

        static::assertCount(3, $collector->getFuncCalls());
    }

    public function test_collects_schema_qualified_functions(): void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT pg_catalog.now(), pg_catalog.current_user()'));

        static::assertCount(2, $collector->getFuncCalls());

        foreach ($collector->getFuncCalls() as $funcCall) {
            static::assertCount(2, $funcCall->getFuncname());
        }
    }

    public function test_enter_returns_null(): void
    {
        $collector = new FuncCallCollector();
        $funcCall = new FuncCall();

        static::assertNull($collector->enter($funcCall));
    }

    public function test_get_func_calls_returns_empty_array_initially(): void
    {
        $collector = new FuncCallCollector();

        static::assertSame([], $collector->getFuncCalls());
    }

    public function test_leave_returns_null(): void
    {
        $collector = new FuncCallCollector();
        $funcCall = new FuncCall();

        static::assertNull($collector->leave($funcCall));
    }

    public function test_node_classes_returns_func_call_class(): void
    {
        static::assertSame([FuncCall::class], FuncCallCollector::nodeClasses());
    }

    public function test_reset_clears_collected_func_calls(): void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);
        $traverser->traverse($this->parseQuery('SELECT COUNT(*), MAX(id) FROM users'));

        static::assertCount(2, $collector->getFuncCalls());

        $collector->reset();

        static::assertSame([], $collector->getFuncCalls());
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
