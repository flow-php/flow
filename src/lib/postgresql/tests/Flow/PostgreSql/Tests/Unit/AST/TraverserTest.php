<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST;

use Flow\PostgreSql\AST\ModificationContext;
use Flow\PostgreSql\AST\NodeModifier;
use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\AST\Traverser;
use Flow\PostgreSql\AST\Visitors\ColumnRefCollector;
use Flow\PostgreSql\AST\Visitors\FuncCallCollector;
use Flow\PostgreSql\AST\Visitors\ParamRefCollector;
use Flow\PostgreSql\AST\Visitors\RangeVarCollector;
use Flow\PostgreSql\Exception\ParserException;
use Flow\PostgreSql\Protobuf\AST\A_Const;
use Flow\PostgreSql\Protobuf\AST\A_Expr;
use Flow\PostgreSql\Protobuf\AST\ColumnRef;
use Flow\PostgreSql\Protobuf\AST\InsertStmt;
use Flow\PostgreSql\Protobuf\AST\Integer as PostgreSqlInteger;
use Flow\PostgreSql\Protobuf\AST\IntoClause;
use Flow\PostgreSql\Protobuf\AST\LimitOption;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\OnConflictClause;
use Flow\PostgreSql\Protobuf\AST\ParamRef;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\RangeSubselect;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Protobuf\AST\RawStmt;
use Flow\PostgreSql\Protobuf\AST\ResTarget;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\Protobuf\AST\WindowDef;
use Flow\PostgreSql\Protobuf\AST\WithClause;
use Flow\PostgreSql\Tests\Mother\RemoveResTargetModifier;
use Flow\PostgreSql\Tests\Mother\ReplaceColumnRefModifier;
use Flow\PostgreSql\Tests\Mother\SelectStmtContextSpy;
use PHPUnit\Framework\TestCase;

use function array_map;
use function extension_loaded;
use function Flow\PostgreSql\DSL\sql_parse;
use function iterator_to_array;
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

    public function test_ancestors_are_messages(): void
    {
        $spy = new SelectStmtContextSpy();

        sql_parse('SELECT * FROM (SELECT 1) s')->traverse($spy);

        static::assertSame([[], [SelectStmt::class, RangeSubselect::class]], $spy->ancestors);
        static::assertSame([null, RangeSubselect::class], $spy->parents);
    }

    public function test_depth_counts_every_message_edge_in_cte(): void
    {
        $spy = new SelectStmtContextSpy();

        sql_parse('WITH c AS (SELECT 1) SELECT * FROM c')->traverse($spy);

        static::assertSame([1, 4], $spy->depths);
    }

    public function test_depth_counts_every_message_edge_in_set_operation(): void
    {
        $spy = new SelectStmtContextSpy();

        sql_parse('SELECT 1 UNION SELECT 2')->traverse($spy);

        static::assertSame([1, 2, 2], $spy->depths);
    }

    public function test_first_deciding_modifier_wins(): void
    {
        $replacing = new class implements NodeModifier {
            public static function nodeClasses(): array
            {
                return [ResTarget::class];
            }

            public function modify(object $node, ModificationContext $context): Node
            {
                return (new Node())->setResTarget(new ResTarget());
            }
        };
        $spy = new class implements NodeModifier {
            public int $calls = 0;

            public static function nodeClasses(): array
            {
                return [ResTarget::class];
            }

            public function modify(object $node, ModificationContext $context): null
            {
                $this->calls++;

                return null;
            }
        };

        sql_parse('SELECT a, b FROM t')->traverse($replacing, $spy);

        static::assertSame(0, $spy->calls);
    }

    public function test_handlers_fire_for_messages_outside_node_wrappers(): void
    {
        $counter = new class implements NodeVisitor {
            /**
             * @var array<class-string, int>
             */
            public array $entered = [];

            public static function nodeClasses(): array
            {
                return [WithClause::class, WindowDef::class, OnConflictClause::class, IntoClause::class];
            }

            public function enter(object $node): ?int
            {
                $this->entered[$node::class] = ($this->entered[$node::class] ?? 0) + 1;

                return null;
            }

            public function leave(object $node): ?int
            {
                return null;
            }
        };

        sql_parse('WITH c AS (SELECT 1 AS a) SELECT count(*) OVER (PARTITION BY a) INTO x FROM c')->traverse($counter);
        sql_parse('INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO NOTHING')->traverse($counter);

        static::assertSame(
            [IntoClause::class => 1, WindowDef::class => 1, WithClause::class => 1, OnConflictClause::class => 1],
            $counter->entered,
        );
    }

    public function test_modifier_replaces_node_in_nested_positions(): void
    {
        static::assertSame(
            'SELECT $99 FROM t WHERE $99 = 1',
            sql_parse('SELECT a FROM t WHERE b = 1')->traverse(new ReplaceColumnRefModifier())->deparse(),
        );
    }

    public function test_modifier_replaces_node_inside_set_operation_arms(): void
    {
        static::assertSame(
            'SELECT $99 FROM t UNION SELECT $99 FROM u',
            sql_parse('SELECT a FROM t UNION SELECT b FROM u')->traverse(new ReplaceColumnRefModifier())->deparse(),
        );
    }

    public function test_modifier_replaces_node_inside_window_definition(): void
    {
        static::assertSame(
            'SELECT count(*) OVER (PARTITION BY $99) FROM t',
            sql_parse('SELECT count(*) OVER (PARTITION BY c) FROM t')
                ->traverse(new ReplaceColumnRefModifier())
                ->deparse(),
        );
    }

    public function test_modifier_replaces_non_node_slot_with_same_class(): void
    {
        $modifier = new class implements NodeModifier {
            public static function nodeClasses(): array
            {
                return [SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): ?SelectStmt
            {
                $parent = $context->parent();

                if (!$parent instanceof SelectStmt || $parent->getLarg() !== $node) {
                    return null;
                }

                return sql_parse('SELECT 42')->raw()->getStmts()[0]->getStmt()?->getSelectStmt();
            }
        };

        static::assertSame(
            'SELECT 42 UNION SELECT 2',
            sql_parse('SELECT 1 UNION SELECT 2')->traverse($modifier)->deparse(),
        );
    }

    public function test_non_node_replacement_at_node_slot_throws(): void
    {
        $modifier = new class implements NodeModifier {
            public static function nodeClasses(): array
            {
                return [ColumnRef::class];
            }

            public function modify(object $node, ModificationContext $context): ParamRef
            {
                return (new ParamRef())->setNumber(99);
            }
        };

        $this->expectException(ParserException::class);

        sql_parse('SELECT a FROM t')->traverse($modifier);
    }

    public function test_removal_survives_a_later_stop(): void
    {
        $stop = new class implements NodeVisitor {
            public static function nodeClasses(): array
            {
                return [ResTarget::class];
            }

            public function enter(object $node): ?int
            {
                /** @var ResTarget $node */
                return $node->getName() === 'c' ? NodeVisitor::STOP_TRAVERSAL : null;
            }

            public function leave(object $node): ?int
            {
                return null;
            }
        };

        static::assertSame(
            'SELECT 1 AS a, 3 AS c',
            sql_parse('SELECT 1 AS a, 2 AS b, 3 AS c')->traverse(new RemoveResTargetModifier('b'), $stop)->deparse(),
        );
    }

    public function test_remove_node_drops_element_from_list(): void
    {
        static::assertSame(
            'SELECT 1 AS a, 3 AS c',
            sql_parse('SELECT 1 AS a, 2 AS b, 3 AS c')->traverse(new RemoveResTargetModifier('b'))->deparse(),
        );
    }

    public function test_remove_node_on_single_slot_throws_before_visitors(): void
    {
        $remove = new class implements NodeModifier {
            public static function nodeClasses(): array
            {
                return [RangeVar::class];
            }

            public function modify(object $node, ModificationContext $context): int
            {
                return NodeModifier::REMOVE_NODE;
            }
        };
        $spy = new class implements NodeVisitor {
            public int $entered = 0;

            public static function nodeClasses(): array
            {
                return [RangeVar::class];
            }

            public function enter(object $node): ?int
            {
                $this->entered++;

                return null;
            }

            public function leave(object $node): ?int
            {
                return null;
            }
        };

        try {
            sql_parse('INSERT INTO t VALUES (1)')->traverse($remove, $spy);
            static::fail('REMOVE_NODE on a single slot must throw');
        } catch (ParserException $exception) {
            static::assertSame(
                'REMOVE_NODE is allowed only in a list, ' . InsertStmt::class . '::Relation is a single node',
                $exception->getMessage(),
            );
        }

        static::assertSame(0, $spy->entered);
    }

    public function test_replaced_node_is_not_seen_by_later_handlers(): void
    {
        $collector = new ColumnRefCollector();

        sql_parse('SELECT a FROM t WHERE b = 1')->traverse(new ReplaceColumnRefModifier(), $collector);

        static::assertCount(0, $collector->getColumnRefs());
    }

    public function test_replacement_of_wrong_class_throws(): void
    {
        $modifier = new class implements NodeModifier {
            public static function nodeClasses(): array
            {
                return [SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): ?Node
            {
                return $context->depth() === 2 ? new Node() : null;
            }
        };

        $this->expectException(ParserException::class);
        $this->expectExceptionMessage(Node::class . ' cannot replace a node in ' . SelectStmt::class . '::Larg');

        sql_parse('SELECT 1 UNION SELECT 2')->traverse($modifier);
    }

    public function test_replacement_survives_a_later_stop(): void
    {
        $stop = new class implements NodeVisitor {
            public static function nodeClasses(): array
            {
                return [A_Const::class];
            }

            public function enter(object $node): int
            {
                return NodeVisitor::STOP_TRAVERSAL;
            }

            public function leave(object $node): ?int
            {
                return null;
            }
        };

        static::assertSame(
            'SELECT $99 FROM t WHERE b = 1',
            sql_parse('SELECT a FROM t WHERE b = 1')->traverse(new ReplaceColumnRefModifier('a'), $stop)->deparse(),
        );
    }

    public function test_skips_raw_statement_without_statement(): void
    {
        $collector = new RangeVarCollector();
        $parseResult = sql_parse('SELECT a FROM t')->raw();
        $parseResult->setStmts([new RawStmt(), ...iterator_to_array($parseResult->getStmts())]);

        (new Traverser($collector))->traverse($parseResult);

        static::assertSame(
            ['t'],
            array_map(static fn(RangeVar $rangeVar): string => $rangeVar->getRelname(), $collector->getRangeVars()),
        );
    }

    public function test_visit_order_follows_descriptor_order(): void
    {
        $collector = new RangeVarCollector();

        sql_parse('SELECT (SELECT x FROM a) FROM b')->traverse($collector);

        static::assertSame(
            ['a', 'b'],
            array_map(static fn(RangeVar $rangeVar): string => $rangeVar->getRelname(), $collector->getRangeVars()),
        );
    }

    public function test_walks_a_hand_built_five_thousand_deep_expression(): void
    {
        $expression = (new Node())->setParamRef((new ParamRef())->setNumber(1));

        for ($i = 0; $i < 5000; $i++) {
            $expression = (new Node())->setAExpr((new A_Expr())->setLexpr($expression));
        }

        $collector = new ParamRefCollector();

        (new Traverser(
            $collector,
        ))->traverse((new ParseResult())->setStmts([(new RawStmt())->setStmt((new Node())->setSelectStmt((new SelectStmt())->setTargetList([
            (new Node())->setResTarget((new ResTarget())->setVal($expression)),
        ])))]));

        static::assertCount(1, $collector->getParamRefs());
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

    public function test_dont_traverse_children_at_the_root_skips_the_statements(): void
    {
        $visitor = new class implements NodeVisitor {
            /** @var list<string> */
            public array $visits = [];

            public static function nodeClasses(): array
            {
                return [ParseResult::class, SelectStmt::class];
            }

            public function enter(object $node): ?int
            {
                $this->visits[] = 'enter ' . $node::class;

                return $node instanceof ParseResult ? NodeVisitor::DONT_TRAVERSE_CHILDREN : null;
            }

            public function leave(object $node): ?int
            {
                $this->visits[] = 'leave ' . $node::class;

                return null;
            }
        };

        (new Traverser($visitor))->traverse($this->parseQuery('SELECT id FROM users'));

        static::assertSame(['enter ' . ParseResult::class, 'leave ' . ParseResult::class], $visitor->visits);
    }

    public function test_enter_and_leave_at_the_root_wrap_the_statements(): void
    {
        $visitor = new class implements NodeVisitor {
            /** @var list<string> */
            public array $visits = [];

            public static function nodeClasses(): array
            {
                return [ParseResult::class, SelectStmt::class];
            }

            public function enter(object $node): ?int
            {
                $this->visits[] = 'enter ' . $node::class;

                return null;
            }

            public function leave(object $node): ?int
            {
                $this->visits[] = 'leave ' . $node::class;

                return null;
            }
        };

        (new Traverser($visitor))->traverse($this->parseQuery('SELECT id FROM users'));

        static::assertSame(
            [
                'enter ' . ParseResult::class,
                'enter ' . SelectStmt::class,
                'leave ' . SelectStmt::class,
                'leave ' . ParseResult::class,
            ],
            $visitor->visits,
        );
    }

    public function test_enter_at_the_root_can_stop_traversal(): void
    {
        $visitor = new class implements NodeVisitor {
            /** @var list<string> */
            public array $visits = [];

            public static function nodeClasses(): array
            {
                return [ParseResult::class, SelectStmt::class];
            }

            public function enter(object $node): ?int
            {
                $this->visits[] = 'enter ' . $node::class;

                return NodeVisitor::STOP_TRAVERSAL;
            }

            public function leave(object $node): ?int
            {
                $this->visits[] = 'leave ' . $node::class;

                return null;
            }
        };

        (new Traverser($visitor))->traverse($this->parseQuery('SELECT id FROM users'));

        static::assertSame(['enter ' . ParseResult::class], $visitor->visits);
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

    public function test_leave_can_stop_traversal(): void
    {
        $visitor = new class implements NodeVisitor {
            public int $entered = 0;

            public static function nodeClasses(): array
            {
                return [ColumnRef::class];
            }

            public function enter(object $node): ?int
            {
                $this->entered++;

                return null;
            }

            public function leave(object $node): int
            {
                return NodeVisitor::STOP_TRAVERSAL;
            }
        };

        sql_parse('SELECT a, b FROM t')->traverse($visitor);

        static::assertSame(1, $visitor->entered);
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
                    $integer = new PostgreSqlInteger();
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

    public function test_modifier_can_receive_the_whole_query_before_its_statements(): void
    {
        $modifier = new class implements NodeModifier {
            /** @var list<string> */
            public array $received = [];

            public static function nodeClasses(): array
            {
                return [ParseResult::class, SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): null
            {
                $this->received[] = $node::class . '@' . $context->depth();

                return null;
            }
        };

        (new Traverser($modifier))->traverse($this->parseQuery('SELECT id FROM users; SELECT id FROM admins'));

        static::assertSame(
            [ParseResult::class . '@0', SelectStmt::class . '@1', SelectStmt::class . '@1'],
            $modifier->received,
        );
    }

    public function test_modifier_can_return_dont_traverse_children_at_the_root(): void
    {
        $modifier = new class implements NodeModifier {
            public int $statements = 0;

            public static function nodeClasses(): array
            {
                return [ParseResult::class, SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): ?int
            {
                if ($node instanceof ParseResult) {
                    return NodeModifier::DONT_TRAVERSE_CHILDREN;
                }

                $this->statements++;

                return null;
            }
        };

        (new Traverser($modifier))->traverse($this->parseQuery('SELECT id FROM users'));

        static::assertSame(0, $modifier->statements);
    }

    public function test_modifier_can_return_stop_traversal_at_the_root(): void
    {
        $modifier = new class implements NodeModifier {
            public int $statements = 0;

            public static function nodeClasses(): array
            {
                return [ParseResult::class, SelectStmt::class];
            }

            public function modify(object $node, ModificationContext $context): ?int
            {
                if ($node instanceof ParseResult) {
                    return NodeModifier::STOP_TRAVERSAL;
                }

                $this->statements++;

                return null;
            }
        };

        (new Traverser($modifier))->traverse($this->parseQuery('SELECT id FROM users'));

        static::assertSame(0, $modifier->statements);
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

    public function test_param_ref_collector_inside_between(): void
    {
        $collector = new ParamRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM t WHERE t.d BETWEEN $1 AND $2');
        $traverser->traverse($result);

        static::assertCount(2, $collector->getParamRefs());
        static::assertSame(2, $collector->getMaxParamNumber());
    }

    public function test_param_ref_collector_inside_in_list(): void
    {
        $collector = new ParamRefCollector();
        $traverser = new Traverser($collector);

        $result = $this->parseQuery('SELECT * FROM t WHERE t.s IN ($1, $2, $3)');
        $traverser->traverse($result);

        static::assertCount(3, $collector->getParamRefs());
        static::assertSame(3, $collector->getMaxParamNumber());
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
