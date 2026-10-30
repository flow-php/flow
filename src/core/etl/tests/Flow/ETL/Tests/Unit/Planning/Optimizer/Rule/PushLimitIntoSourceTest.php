<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Optimizer\Rule;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Operators\Transformer\AddRowIndex\StartFrom;
use Flow\ETL\Operators\Transformer\AddRowIndexTransformer;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Optimizer\Rule\PushLimitIntoSource;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\ChildlessNode;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure;
use function Flow\ETL\DSL\to_memory;

final class PushLimitIntoSourceTest extends FlowTestCase
{
    public function test_a_limit_directly_above_the_read_is_pushed(): void
    {
        static::assertSame(
            10,
            (new PushLimitIntoSource())
                ->apply(NodeMother::plan(NodeMother::limit(NodeMother::read(), 10)), config()->backend())
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_a_limit_passes_a_select(): void
    {
        static::assertSame(
            10,
            (new PushLimitIntoSource())
                ->apply(
                    NodeMother::plan(NodeMother::limit(NodeMother::select(NodeMother::read()), 10)),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    #[DataProvider('expanding_expressions')]
    public function test_a_limit_does_not_pass_an_expand_column(ScalarFunction $function): void
    {
        static::assertNull(
            (new PushLimitIntoSource())
                ->apply(
                    NodeMother::plan(NodeMother::limit(
                        new Node\ExpandColumn(NodeMother::read(), 'expanded', $function),
                        10,
                    )),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public static function expanding_expressions(): Generator
    {
        yield 'root expand' => [ref('data')->expand()];
        yield 'nested expand' => [structure(['tag' => ref('data')->expand()])];
    }

    public function test_a_limit_does_not_pass_a_filter(): void
    {
        static::assertNull(
            (new PushLimitIntoSource())
                ->apply(
                    NodeMother::plan(NodeMother::limit(
                        new Node\Filter(NodeMother::select(NodeMother::read()), ref('id')->equals(lit(1))),
                        10,
                    )),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_a_limit_does_not_pass_a_write(): void
    {
        static::assertNull(
            (new PushLimitIntoSource())
                ->apply(
                    NodeMother::plan(NodeMother::limit(
                        new Node\Write(NodeMother::read(), to_memory(new ArrayMemory())),
                        10,
                    )),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_a_blocker_discards_the_limit_collected_above_it_and_the_walk_continues(): void
    {
        static::assertSame(
            5,
            (new PushLimitIntoSource())
                ->apply(
                    NodeMother::plan(NodeMother::limit(
                        new Node\Sort(NodeMother::limit(NodeMother::read(), 5), [ref('id')->asc()], memory_sort()),
                        3,
                    )),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_the_push_rewrites_the_leaf_and_keeps_the_spine(): void
    {
        $extractor = new RecordingFileExtractor(schema(int_schema('id')));
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::select(new Read($extractor)), 10));

        $pushed = (new PushLimitIntoSource())->apply($plan, config()->backend());

        static::assertNotSame($plan, $pushed);
        $limit = $pushed->root->children()[0];
        static::assertInstanceOf(Node\Limit::class, $limit);
        static::assertSame(10, $limit->limit);
        static::assertInstanceOf(Node\Select::class, $limit->children()[0]);
        static::assertSame(10, $pushed->source()->criteria()->limit);
        static::assertSame($extractor, $pushed->source()->extractor());
        static::assertNull($plan->source()->criteria()->limit);
        static::assertSame([], $extractor->criteria);
    }

    public function test_the_minimum_of_two_limits_is_pushed(): void
    {
        static::assertSame(
            3,
            (new PushLimitIntoSource())
                ->apply(
                    NodeMother::plan(NodeMother::limit(
                        NodeMother::select(NodeMother::limit(NodeMother::read(), 10)),
                        3,
                    )),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_a_limit_narrows_an_already_pushed_limit(): void
    {
        static::assertSame(
            4,
            (new PushLimitIntoSource())
                ->apply(
                    NodeMother::plan(NodeMother::limit(
                        new Node\Rename(NodeMother::read()->withLimit(4), 'id', 'new_id'),
                        10,
                    )),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_a_plan_without_a_limit_pushes_nothing(): void
    {
        static::assertNull(
            (new PushLimitIntoSource())
                ->apply(NodeMother::plan(NodeMother::select(NodeMother::read())), config()->backend())
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_a_row_input_chain_that_does_not_end_in_a_read_throws(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A logical plan must end in a Read, ' . ChildlessNode::class . ' found');

        (new PushLimitIntoSource())->apply(
            NodeMother::plan(NodeMother::limit(NodeMother::select(new ChildlessNode()), 10)),
            config()->backend(),
        );
    }

    public function test_a_transaction_contributes_one_walk_per_child(): void
    {
        $limit = NodeMother::limit(NodeMother::read(), 3);

        static::assertSame(
            3,
            (new PushLimitIntoSource())
                ->apply(
                    new LogicalPlan(
                        new Outputs(
                            new Result($limit),
                            new Node\Transaction(
                                new RecordingTransaction(),
                                new Write($limit, to_memory(new ArrayMemory())),
                                new Write($limit, to_memory(new ArrayMemory())),
                            ),
                        ),
                    ),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_one_transaction_child_whose_walk_yields_no_limit_blocks_the_push(): void
    {
        $read = NodeMother::read();
        $limit = NodeMother::limit($read, 3);

        static::assertNull(
            (new PushLimitIntoSource())
                ->apply(
                    new LogicalPlan(
                        new Outputs(
                            new Result($limit),
                            new Node\Transaction(
                                new RecordingTransaction(),
                                new Write($limit, to_memory(new ArrayMemory())),
                                new Write($read, to_memory(new ArrayMemory())),
                            ),
                        ),
                    ),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_a_limit_below_a_write_is_pushed(): void
    {
        $limit = NodeMother::limit(NodeMother::read(), 3);

        static::assertSame(
            3,
            (new PushLimitIntoSource())
                ->apply(
                    new LogicalPlan(new Outputs(new Result($limit), new Write($limit, to_memory(new ArrayMemory())))),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_a_limit_above_a_write_does_not_narrow_the_write(): void
    {
        $read = NodeMother::read();

        static::assertNull(
            (new PushLimitIntoSource())
                ->apply(
                    new LogicalPlan(
                        new Outputs(
                            new Result(NodeMother::limit($read, 3)),
                            new Write($read, to_memory(new ArrayMemory())),
                        ),
                    ),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_the_widest_limit_of_every_root_is_pushed(): void
    {
        $limit = NodeMother::limit(NodeMother::read(), 5);

        static::assertSame(
            5,
            (new PushLimitIntoSource())
                ->apply(
                    new LogicalPlan(
                        new Outputs(
                            new Result(NodeMother::limit($limit, 3)),
                            new Write($limit, to_memory(new ArrayMemory())),
                        ),
                    ),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_a_write_whose_walk_yields_no_limit_blocks_the_push(): void
    {
        $read = NodeMother::read();

        static::assertNull(
            (new PushLimitIntoSource())
                ->apply(
                    new LogicalPlan(
                        new Outputs(
                            new Result(NodeMother::limit($read, 3)),
                            new Write(
                                new Node\Transform($read, new AddRowIndexTransformer('idx', StartFrom::ZERO)),
                                to_memory(new ArrayMemory()),
                            ),
                        ),
                    ),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    public function test_the_push_reaches_the_writes_through_the_shared_leaf(): void
    {
        $limit = NodeMother::limit(NodeMother::read(), 3);

        $plan = (new PushLimitIntoSource())->apply(
            new LogicalPlan(new Outputs(new Result($limit), new Write($limit, to_memory(new ArrayMemory())))),
            config()->backend(),
        );

        static::assertSame($plan->root->children()[0]->children()[0], $plan->writes()->all()[0]->children()[0]);
    }

    public function test_a_limit_above_an_offset_pushes_the_limit_plus_the_offset(): void
    {
        static::assertSame(
            110,
            (new PushLimitIntoSource())
                ->apply(
                    NodeMother::plan(NodeMother::limit(new Node\Offset(NodeMother::read(), 100), 10)),
                    config()->backend(),
                )
                ->source()
                ->criteria()
                ->limit,
        );
    }

    /**
     * null means "do not push": either no limit survived, or the chain does not end at the leaf.
     */
    #[DataProvider('walks')]
    public function test_the_limit_one_root_lets_through_to_the_leaf(Node $from, Read $leaf, ?int $expected): void
    {
        static::assertSame($expected, (new PushLimitIntoSource())->limitAt($from, $leaf));
    }

    public static function walks(): Generator
    {
        $read = NodeMother::read();

        yield 'a limit above the read' => [NodeMother::limit($read, 3), $read, 3];
        yield 'two limits fold with min' => [NodeMother::limit(NodeMother::limit($read, 5), 3), $read, 3];
        yield 'a blocker discards the limit above it' => [
            NodeMother::limit(NodeMother::sort(NodeMother::limit($read, 5)), 3),
            $read,
            5,
        ];
        yield 'an opaque transform discards the limit above it' => [
            NodeMother::limit(new Node\Transform($read, new AddRowIndexTransformer('idx', StartFrom::ZERO)), 3),
            $read,
            null,
        ];
        yield 'a chain ending at a childless node that is not the leaf' => [
            NodeMother::limit(new ChildlessNode(), 3),
            $read,
            null,
        ];
        yield 'a limit below an opaque transform passes' => [
            new Node\Transform(NodeMother::limit($read, 3), new AddRowIndexTransformer('idx', StartFrom::ZERO)),
            $read,
            3,
        ];
        yield 'a limit above an offset grows by the skipped rows' => [
            NodeMother::limit(new Node\Offset($read, 100), 10),
            $read,
            110,
        ];
        yield 'an offset above a limit leaves the limit' => [new Node\Offset(NodeMother::limit($read, 5), 2), $read, 5];
        yield 'an offset without a limit pushes nothing' => [new Node\Offset($read, 100), $read, null];
        yield 'a top-n discards the limit above it' => [
            NodeMother::limit(new Node\TopN($read, [ref('id')->asc()], 5), 3),
            $read,
            null,
        ];
        yield 'the leaf alone' => [$read, $read, null];
    }
}
