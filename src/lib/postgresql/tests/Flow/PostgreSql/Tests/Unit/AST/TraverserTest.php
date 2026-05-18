<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\AST\Traverser;
use Flow\PostgreSql\AST\Visitors\ColumnRefCollector;
use Flow\PostgreSql\AST\Visitors\FuncCallCollector;
use Flow\PostgreSql\AST\Visitors\RangeVarCollector;
use Flow\PostgreSql\Protobuf\AST\A_Const;
use Flow\PostgreSql\Protobuf\AST\ColumnRef;
use Flow\PostgreSql\Protobuf\AST\LimitOption;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use PHPUnit\Framework\TestCase;

use function array_map;
use function extension_loaded;
use function pg_query_deparse;
use function pg_query_parse;

final class TraverserTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_column_ref_collector(): void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT id, name FROM users');
        $traverser->traverse($result);

        static::assertCount(2, $collector->getColumnRefs());
    }

    public function test_column_ref_collector_from_join_condition(): void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM users u JOIN orders o ON u.id = o.user_id');
        $traverser->traverse($result);

        static::assertCount(3, $collector->getColumnRefs());
    }

    public function test_column_ref_collector_from_order_by(): void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT id FROM users ORDER BY name');
        $traverser->traverse($result);

        static::assertCount(2, $collector->getColumnRefs());
    }

    public function test_column_ref_collector_from_subquery(): void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM (SELECT id FROM users) sub');
        $traverser->traverse($result);

        static::assertCount(2, $collector->getColumnRefs());
    }

    public function test_column_ref_collector_from_where_clause(): void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT 1 FROM users WHERE active = true AND status = 1');
        $traverser->traverse($result);

        static::assertCount(2, $collector->getColumnRefs());
    }

    public function test_column_ref_collector_with_table_qualifier(): void
    {
        $collector = new ColumnRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT u.id, u.name FROM users u');
        $traverser->traverse($result);

        static::assertCount(2, $collector->getColumnRefs());

        foreach ($collector->getColumnRefs() as $ref) {
            $fields = $ref->getFields();
            static::assertCount(2, $fields);
        }
    }

    public function test_dont_traverse_children(): void
    {
        $visitor = new class implements NodeVisitor {
            public int $nodeCount = 0;

            public static function nodeClasses(): array
            {
                return [SelectStmt::class];
            }

            public function enter(object $node): int
            {
                $this->nodeCount++;

                return NodeVisitor::DONT_TRAVERSE_CHILDREN;
            }

            public function leave(object $node): ?int
            {
                return null;
            }
        };

        $traverser = new Traverser($visitor);
        $result = $this->parseQuery('SELECT id, name FROM users');
        $traverser->traverse($result);

        static::assertSame(1, $visitor->nodeCount);
    }

    public function test_func_call_collector(): void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT COUNT(*), MAX(id) FROM users');
        $traverser->traverse($result);

        static::assertCount(2, $collector->getFuncCalls());
    }

    public function test_func_call_collector_nested(): void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT UPPER(TRIM(name)) FROM users');
        $traverser->traverse($result);

        static::assertCount(2, $collector->getFuncCalls());
    }

    public function test_func_call_collector_with_schema(): void
    {
        $collector = new FuncCallCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT pg_catalog.now()');
        $traverser->traverse($result);

        static::assertCount(1, $collector->getFuncCalls());

        $funcname = $collector->getFuncCalls()[0]->getFuncname();
        static::assertCount(2, $funcname);
    }

    public function test_modifier_can_mutate_node_in_place(): void
    {
        $modifier = new class implements NodeModifier {
            public static function nodeClasses(): array
            {
                return [SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): int|object|null
            {
                if (!$node instanceof SelectStmt) {
                    return null;
                }

                if ($context->isTopLevel()) {
                    $integer = new \Flow\PostgreSql\Protobuf\AST\Integer();
                    $integer->setIval(10);

                    $aConst = new A_Const(['ival' => $integer]);

                    $limitNode = new Node();
                    $limitNode->setAConst($aConst);

                    $node->setLimitOption(LimitOption::LIMIT_OPTION_COUNT);
                    $node->setLimitCount($limitNode);
                }

                return null;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM users');
        $traverser->traverse($result);

        $deparsed = pg_query_deparse($result->serializeToString());

        static::assertStringContainsString('LIMIT 10', $deparsed);
    }

    public function test_modifier_can_skip_children(): void
    {
        $modifyCount = 0;

        $modifier = new class($modifyCount) implements NodeModifier {
            private int $count;

            public function __construct(int &$count)
            {
                $this->count = &$count;
            }

            public static function nodeClasses(): array
            {
                return [SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): int
            {
                $this->count++;

                return NodeModifier::DONT_TRAVERSE_CHILDREN;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM (SELECT id FROM users) sub');
        $traverser->traverse($result);

        static::assertSame(1, $modifyCount);
    }

    public function test_modifier_can_stop_traversal(): void
    {
        $modifyCount = 0;

        $modifier = new class($modifyCount) implements NodeModifier {
            private int $count;

            public function __construct(int &$count)
            {
                $this->count = &$count;
            }

            public static function nodeClasses(): array
            {
                return [SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): int
            {
                $this->count++;

                return NodeModifier::STOP_TRAVERSAL;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM (SELECT id FROM users) sub');
        $traverser->traverse($result);

        static::assertSame(1, $modifyCount);
    }

    public function test_modifier_is_top_level_check(): void
    {
        $topLevelResults = [];

        $modifier = new class($topLevelResults) implements NodeModifier {
            /**
             * @param array<bool> $results
             *
             * @phpstan-ignore property.onlyWritten (accessed via reference)
             */
            public function __construct(
                private array &$results,
            ) {}

            public static function nodeClasses(): array
            {
                return [SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): null
            {
                $this->results[] = $context->isTopLevel();

                return null;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM (SELECT id FROM users) sub');
        $traverser->traverse($result);

        static::assertContains(true, $topLevelResults);
        static::assertContains(false, $topLevelResults);
    }

    public function test_modifier_receives_context_with_nested_depth(): void
    {
        $depths = [];

        $modifier = new class($depths) implements NodeModifier {
            /**
             * @param array<int> $depths
             *
             * @phpstan-ignore property.onlyWritten (accessed via reference)
             */
            public function __construct(
                private array &$depths,
            ) {}

            public static function nodeClasses(): array
            {
                return [SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): null
            {
                $this->depths[] = $context->depth();

                return null;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM (SELECT id FROM users) sub');
        $traverser->traverse($result);

        static::assertCount(2, $depths);
        static::assertSame(1, $depths[0]);
        static::assertGreaterThan(1, $depths[1]);
    }

    public function test_modifier_receives_context_with_top_level_depth(): void
    {
        $depths = [];

        $modifier = new class($depths) implements NodeModifier {
            /**
             * @param array<int> $depths
             *
             * @phpstan-ignore property.onlyWritten (accessed via reference)
             */
            public function __construct(
                private array &$depths,
            ) {}

            public static function nodeClasses(): array
            {
                return [SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): null
            {
                $this->depths[] = $context->depth();

                return null;
            }
        };

        $traverser = new Traverser($modifier);
        $result = $this->parseQuery('SELECT * FROM users');
        $traverser->traverse($result);

        static::assertContains(1, $depths);
        static::assertTrue($depths[0] === 1);
    }

    public function test_multiple_visitors(): void
    {
        $columnCollector = new ColumnRefCollector();
        $funcCollector = new FuncCallCollector();
        $rangeVarCollector = new RangeVarCollector();

        $traverser = new Traverser($columnCollector, $funcCollector, $rangeVarCollector);

        $result = $this->parseQuery('SELECT COUNT(id), name FROM users WHERE active = true');
        $traverser->traverse($result);

        static::assertCount(3, $columnCollector->getColumnRefs());
        static::assertCount(1, $funcCollector->getFuncCalls());
        static::assertCount(1, $rangeVarCollector->getRangeVars());
    }

    public function test_range_var_collector(): void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM users');
        $traverser->traverse($result);

        static::assertCount(1, $collector->getRangeVars());
        static::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_range_var_collector_from_cte(): void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active');
        $traverser->traverse($result);

        static::assertCount(2, $collector->getRangeVars());

        $tableNames = array_map(static fn($rv) => $rv->getRelname(), $collector->getRangeVars());
        static::assertContains('users', $tableNames);
        static::assertContains('active', $tableNames);
    }

    public function test_range_var_collector_from_delete(): void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('DELETE FROM users WHERE id = 1');
        $traverser->traverse($result);

        static::assertCount(1, $collector->getRangeVars());
        static::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_range_var_collector_from_insert(): void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('INSERT INTO users (name) VALUES (\'john\')');
        $traverser->traverse($result);

        static::assertCount(1, $collector->getRangeVars());
        static::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_range_var_collector_from_join(): void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM users u JOIN orders o ON u.id = o.user_id');
        $traverser->traverse($result);

        static::assertCount(2, $collector->getRangeVars());
    }

    public function test_range_var_collector_from_subquery(): void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM (SELECT * FROM users) sub');
        $traverser->traverse($result);

        static::assertCount(1, $collector->getRangeVars());
        static::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_range_var_collector_from_update(): void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('UPDATE users SET name = \'john\' WHERE id = 1');
        $traverser->traverse($result);

        static::assertCount(1, $collector->getRangeVars());
        static::assertSame('users', $collector->getRangeVars()[0]->getRelname());
    }

    public function test_range_var_collector_with_alias(): void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM users AS u');
        $traverser->traverse($result);

        $rangeVars = $collector->getRangeVars();
        static::assertCount(1, $rangeVars);
        static::assertSame('users', $rangeVars[0]->getRelname());
        $alias = $rangeVars[0]->getAlias();
        static::assertNotNull($alias);
        static::assertSame('u', $alias->getAliasname());
    }

    public function test_range_var_collector_with_schema(): void
    {
        $collector = new RangeVarCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM public.users');
        $traverser->traverse($result);

        static::assertCount(1, $collector->getRangeVars());
        static::assertSame('users', $collector->getRangeVars()[0]->getRelname());
        static::assertSame('public', $collector->getRangeVars()[0]->getSchemaname());
    }

    public function test_stop_traversal(): void
    {
        $visitor = new class implements NodeVisitor {
            public int $nodeCount = 0;

            public static function nodeClasses(): array
            {
                return [ColumnRef::class];
            }

            public function enter(object $node): ?int
            {
                $this->nodeCount++;

                if ($this->nodeCount >= 2) {
                    return NodeVisitor::STOP_TRAVERSAL;
                }

                return null;
            }

            public function leave(object $node): ?int
            {
                return null;
            }
        };

        $traverser = new Traverser($visitor);
        $result = $this->parseQuery('SELECT id, name, email FROM users');
        $traverser->traverse($result);

        static::assertSame(2, $visitor->nodeCount);
    }

    public function test_traverser_accepts_both_visitors_and_modifiers(): void
    {
        $collector = new ColumnRefCollector();

        $modifier = new class implements NodeModifier {
            public static function nodeClasses(): array
            {
                return [SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): int|object|null
            {
                return null;
            }
        };

        $traverser = new Traverser($collector, $modifier);
        $result = $this->parseQuery('SELECT id, name FROM users');
        $traverser->traverse($result);

        static::assertCount(2, $collector->getColumnRefs());
    }

    public function test_traverser_without_visitors(): void
    {
        $traverser = new Traverser();
        $result = $this->parseQuery('SELECT id FROM users');

        $traverser->traverse($result);

        $this->expectNotToPerformAssertions();
    }

    private function parseQuery(string $sql): ParseResult
    {
        $json = pg_query_parse($sql);
        $result = new ParseResult();
        $result->mergeFromJsonString($json);

        return $result;
    }
}
