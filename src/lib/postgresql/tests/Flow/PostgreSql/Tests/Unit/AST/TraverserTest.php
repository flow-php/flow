<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST;

use Flow\PostgreSql\AST\{ModificationContext, NodeModifier, NodeVisitor, Traverser};
use Flow\PostgreSql\AST\Visitors\{ColumnRefCollector, FuncCallCollector, RangeVarCollector};
use Flow\PostgreSql\Protobuf\AST\{ColumnRef, Node, ParseResult, SelectStmt};
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

    public function test_modifier_can_mutate_node_in_place() : void
    {
        $modifier = new class implements NodeModifier {
            public static function nodeClass() : string
            {
                return SelectStmt::class;
            }

            public function modify(object $node, ModificationContext $context) : int|object|null
            {
                /** @var SelectStmt $node */
                if ($context->isTopLevel()) {
                    $integer = new \Flow\PostgreSql\Protobuf\AST\Integer();
                    $integer->setIval(10);

                    $aConst = new \Flow\PostgreSql\Protobuf\AST\A_Const();
                    /** @phpstan-ignore argument.type */
                    $aConst->setIval($integer);

                    $limitNode = new Node();
                    $limitNode->setAConst($aConst);

                    $node->setLimitOption(\Flow\PostgreSql\Protobuf\AST\LimitOption::LIMIT_OPTION_COUNT);
                    $node->setLimitCount($limitNode);
                }

                return null;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM users');
        $traverser->traverse($result);

        $deparsed = \pg_query_deparse($result->serializeToString());

        self::assertStringContainsString('LIMIT 10', $deparsed);
    }

    public function test_modifier_can_skip_children() : void
    {
        $modifyCount = 0;

        $modifier = new class($modifyCount) implements NodeModifier {
            private int $count;

            public function __construct(int &$count)
            {
                $this->count = &$count;
            }

            public static function nodeClass() : string
            {
                return SelectStmt::class;
            }

            public function modify(object $node, ModificationContext $context) : int
            {
                $this->count++;

                return NodeModifier::DONT_TRAVERSE_CHILDREN;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM (SELECT id FROM users) sub');
        $traverser->traverse($result);

        self::assertSame(1, $modifyCount);
    }

    public function test_modifier_can_stop_traversal() : void
    {
        $modifyCount = 0;

        $modifier = new class($modifyCount) implements NodeModifier {
            private int $count;

            public function __construct(int &$count)
            {
                $this->count = &$count;
            }

            public static function nodeClass() : string
            {
                return SelectStmt::class;
            }

            public function modify(object $node, ModificationContext $context) : int
            {
                $this->count++;

                return NodeModifier::STOP_TRAVERSAL;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM (SELECT id FROM users) sub');
        $traverser->traverse($result);

        self::assertSame(1, $modifyCount);
    }

    public function test_modifier_is_top_level_check() : void
    {
        /** @var array<bool> $topLevelResults */
        $topLevelResults = [];

        $modifier = new class($topLevelResults) implements NodeModifier {
            /**
             * @param array<bool> $results
             *
             * @phpstan-ignore property.onlyWritten (accessed via reference)
             */
            public function __construct(private array &$results)
            {
            }

            public static function nodeClass() : string
            {
                return SelectStmt::class;
            }

            public function modify(object $node, ModificationContext $context) : null
            {
                $this->results[] = $context->isTopLevel();

                return null;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM (SELECT id FROM users) sub');
        $traverser->traverse($result);

        self::assertContains(true, $topLevelResults);
        self::assertContains(false, $topLevelResults);
    }

    public function test_modifier_receives_context_with_nested_depth() : void
    {
        /** @var array<int> $depths */
        $depths = [];

        $modifier = new class($depths) implements NodeModifier {
            /**
             * @param array<int> $depths
             *
             * @phpstan-ignore property.onlyWritten (accessed via reference)
             */
            public function __construct(private array &$depths)
            {
            }

            public static function nodeClass() : string
            {
                return SelectStmt::class;
            }

            public function modify(object $node, ModificationContext $context) : null
            {
                $this->depths[] = $context->depth();

                return null;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM (SELECT id FROM users) sub');
        $traverser->traverse($result);

        self::assertCount(2, $depths);
        self::assertSame(1, $depths[0]);
        self::assertGreaterThan(1, $depths[1]);
    }

    public function test_modifier_receives_context_with_top_level_depth() : void
    {
        /** @var array<int> $depths */
        $depths = [];

        $modifier = new class($depths) implements NodeModifier {
            /**
             * @param array<int> $depths
             *
             * @phpstan-ignore property.onlyWritten (accessed via reference)
             */
            public function __construct(private array &$depths)
            {
            }

            public static function nodeClass() : string
            {
                return SelectStmt::class;
            }

            public function modify(object $node, ModificationContext $context) : null
            {
                $this->depths[] = $context->depth();

                return null;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM users');
        $traverser->traverse($result);

        self::assertContains(1, $depths);
        self::assertTrue($depths[0] === 1);
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

    public function test_traverser_accepts_both_visitors_and_modifiers() : void
    {
        $collector = new ColumnRefCollector();

        $modifier = new class implements NodeModifier {
            public static function nodeClass() : string
            {
                return SelectStmt::class;
            }

            public function modify(object $node, ModificationContext $context) : int|object|null
            {
                return null;
            }
        };

        $traverser = new Traverser($collector, $modifier);
        $result = $this->parseQuery('SELECT id, name FROM users');
        $traverser->traverse($result);

        self::assertCount(2, $collector->getColumnRefs());
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
