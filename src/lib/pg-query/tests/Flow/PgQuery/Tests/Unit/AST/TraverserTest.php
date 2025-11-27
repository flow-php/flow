<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\AST;

use Flow\PgQuery\AST\{NodeVisitor, Traverser};
use Flow\PgQuery\AST\Visitors\{ColumnRefCollector, FuncCallCollector, RangeVarCollector};
use Flow\PgQuery\Protobuf\AST\{ColumnRef, ParseResult, SelectStmt};
use PHPUnit\Framework\TestCase;

final class TraverserTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_column_ref_collector() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT id, name FROM users');
        $traverser->traverse($result);

        self::assertCount(2, $collector->getColumnRefs());
    }

    public function test_column_ref_collector_from_join_condition() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM users u JOIN orders o ON u.id = o.user_id');
        $traverser->traverse($result);

        self::assertCount(3, $collector->getColumnRefs());
    }

    public function test_column_ref_collector_from_order_by() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT id FROM users ORDER BY name');
        $traverser->traverse($result);

        self::assertCount(2, $collector->getColumnRefs());
    }

    public function test_column_ref_collector_from_subquery() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM (SELECT id FROM users) sub');
        $traverser->traverse($result);

        self::assertCount(2, $collector->getColumnRefs());
    }

    public function test_column_ref_collector_from_where_clause() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT 1 FROM users WHERE active = true AND status = 1');
        $traverser->traverse($result);

        self::assertCount(2, $collector->getColumnRefs());
    }

    public function test_column_ref_collector_with_table_qualifier() : void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT u.id, u.name FROM users u');
        $traverser->traverse($result);

        self::assertCount(2, $collector->getColumnRefs());

        foreach ($collector->getColumnRefs() as $ref) {
            $fields = $ref->getFields();
            self::assertCount(2, $fields);
        }
    }

    public function test_dont_traverse_children() : void
    {
        $visitor = new class implements NodeVisitor {
            public int $nodeCount = 0;

            public static function nodeClass() : string
            {
                return SelectStmt::class;
            }

            public function enter(object $node) : int
            {
                $this->nodeCount++;

                return NodeVisitor::DONT_TRAVERSE_CHILDREN;
            }

            public function leave(object $node) : ?int
            {
                return null;
            }
        };

        $traverser = new Traverser($visitor);
        $result = $this->parseQuery('SELECT id, name FROM users');
        $traverser->traverse($result);

        self::assertSame(1, $visitor->nodeCount);
    }

    public function test_func_call_collector() : void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT COUNT(*), MAX(id) FROM users');
        $traverser->traverse($result);

        self::assertCount(2, $collector->getFuncCalls());
    }

    public function test_func_call_collector_nested() : void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT UPPER(TRIM(name)) FROM users');
        $traverser->traverse($result);

        self::assertCount(2, $collector->getFuncCalls());
    }

    public function test_func_call_collector_with_schema() : void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT pg_catalog.now()');
        $traverser->traverse($result);

        self::assertCount(1, $collector->getFuncCalls());

        $funcname = $collector->getFuncCalls()[0]->getFuncname();
        self::assertCount(2, $funcname);
    }

    public function test_multiple_visitors() : void
    {
        $columnCollector = new ColumnRefCollector();
        $funcCollector = new FuncCallCollector();
        $rangeVarCollector = new RangeVarCollector();

        $traverser = new Traverser($columnCollector, $funcCollector, $rangeVarCollector);

        $result = $this->parseQuery('SELECT COUNT(id), name FROM users WHERE active = true');
        $traverser->traverse($result);

        self::assertCount(3, $columnCollector->getColumnRefs());
        self::assertCount(1, $funcCollector->getFuncCalls());
        self::assertCount(1, $rangeVarCollector->getRangeVars());
    }

    public function test_range_var_collector() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM users');
        $traverser->traverse($result);

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_range_var_collector_from_cte() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active');
        $traverser->traverse($result);

        self::assertCount(2, $collector->getRangeVars());

        $tableNames = \array_map(fn ($rv) => $rv->getRelname(), $collector->getRangeVars());
        self::assertContains('users', $tableNames);
        self::assertContains('active', $tableNames);
    }

    public function test_range_var_collector_from_delete() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('DELETE FROM users WHERE id = 1');
        $traverser->traverse($result);

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_range_var_collector_from_insert() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('INSERT INTO users (name) VALUES (\'john\')');
        $traverser->traverse($result);

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_range_var_collector_from_join() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM users u JOIN orders o ON u.id = o.user_id');
        $traverser->traverse($result);

        self::assertCount(2, $collector->getRangeVars());
    }

    public function test_range_var_collector_from_subquery() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM (SELECT * FROM users) sub');
        $traverser->traverse($result);

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_range_var_collector_from_update() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('UPDATE users SET name = \'john\' WHERE id = 1');
        $traverser->traverse($result);

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_range_var_collector_with_alias() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM users AS u');
        $traverser->traverse($result);

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
        self::assertNotNull($collector->getRangeVars()[0]->getAlias());
        self::assertSame('u', $collector->getRangeVars()[0]->getAlias()->getAliasname());
    }

    public function test_range_var_collector_with_schema() : void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM public.users');
        $traverser->traverse($result);

        self::assertCount(1, $collector->getRangeVars());
        self::assertSame('users', $collector->getRangeVars()[0]->getRelname());
        self::assertSame('public', $collector->getRangeVars()[0]->getSchemaname());
    }

    public function test_stop_traversal() : void
    {
        $visitor = new class implements NodeVisitor {
            public int $nodeCount = 0;

            public static function nodeClass() : string
            {
                return ColumnRef::class;
            }

            public function enter(object $node) : ?int
            {
                $this->nodeCount++;

                if ($this->nodeCount >= 2) {
                    return NodeVisitor::STOP_TRAVERSAL;
                }

                return null;
            }

            public function leave(object $node) : ?int
            {
                return null;
            }
        };

        $traverser = new Traverser($visitor);
        $result = $this->parseQuery('SELECT id, name, email FROM users');
        $traverser->traverse($result);

        self::assertSame(2, $visitor->nodeCount);
    }

    public function test_traverser_without_visitors() : void
    {
        $traverser = new Traverser();
        $result = $this->parseQuery('SELECT id FROM users');

        $traverser->traverse($result);

        $this->expectNotToPerformAssertions();
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
