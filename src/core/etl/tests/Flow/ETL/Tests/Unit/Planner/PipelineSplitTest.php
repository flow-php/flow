<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Executor;
use Flow\ETL\Executor\Raw;
use Flow\ETL\Executor\SinkFeed;
use Flow\ETL\Executor\SourceRows;
use Flow\ETL\Executor\TransactionalSinks;
use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Loader\ArrayLoader;
use Flow\ETL\Loader\MemoryLoader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Collect;
use Flow\ETL\Plan\Node\JoinEach;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Plan\Trigger;
use Flow\ETL\Planner;
use Flow\ETL\Planner\PipelineSplit;
use Flow\ETL\Planner\PlannedNode;
use Flow\ETL\Planner\PlannedNodes;
use Flow\ETL\Processor\BatchingByProcessor;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Tests\Context\PipelineSteps;
use Flow\ETL\Tests\Double\ChildlessNode;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\SpyTransformer;
use Flow\ETL\Tests\Double\StaticDataFrameFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function array_map;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;
use function Flow\ETL\DSL\to_memory;

final class PipelineSplitTest extends FlowTestCase
{
    public function test_a_plan_ending_in_a_blocking_node_stays_one_pipeline(): void
    {
        $root = new Collect(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))));
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        $plan = (new PipelineSplit())->of($logical, $planned, NodeMother::context());

        static::assertSame(0, $plan->root()->id);
        static::assertNull($plan->root()->input());
        static::assertSame([CollectingProcessor::class], PipelineSteps::classes($plan->root()->segments()));
    }

    public function test_ids_count_up_from_the_leaf_and_the_root_has_the_highest(): void
    {
        $root = NodeMother::sort(
            new Collect(NodeMother::sort(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))))),
            refs(ref('id')),
        );
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        $plan = (new PipelineSplit())->of($logical, $planned, NodeMother::context());

        static::assertSame(2, $plan->root()->id);
        static::assertSame(1, $plan->root()->input()?->id);
        static::assertSame(0, $plan->root()->input()?->input()?->id);
        static::assertNull($plan->root()->input()?->input()?->input());
    }

    public function test_the_upstream_pipeline_carries_a_trailing_empty_segment(): void
    {
        $root = NodeMother::limit(
            new Node\Sort(
                NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
                refs(ref('id')),
                memory_sort(),
            ),
            5,
        );
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        $segments = (new PipelineSplit())
            ->of($logical, $planned, NodeMother::context())
            ->root()
            ->input()
            ?->segments()
            ->all() ?? [];

        static::assertArrayHasKey(1, $segments);

        $trailing = $segments[1];

        static::assertSame([], $trailing->steps());
        static::assertNull($trailing->processor());
        static::assertNull($trailing->extractor());
    }

    public function test_steps_go_in_through_segments_add_so_a_processor_opens_a_new_segment(): void
    {
        $output = [];
        $root = new Write(
            NodeMother::select(
                new Node\Batch(
                    NodeMother::select(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
                    10,
                ),
            ),
            to_array($output),
        );
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        $segments = (new PipelineSplit())
            ->of($logical, $planned, NodeMother::context())
            ->root()
            ->segments()
            ->all();

        static::assertCount(2, $segments);
        static::assertSame(
            [SelectEntriesTransformer::class],
            array_map(static fn($step) => $step::class, $segments[0]->steps()),
        );
        static::assertNotNull($segments[0]->processor());
        static::assertSame(
            [SelectEntriesTransformer::class, ArrayLoader::class],
            array_map(static fn($step) => $step::class, $segments[1]->steps()),
        );
    }

    public function test_a_refused_plan_uses_raw_steps_for_every_node(): void
    {
        $joinEach = new JoinEach(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            new StaticDataFrameFactory(df()->read(from_array([['id' => 1]], schema(int_schema('id'))))),
            join_on(['id' => 'id']),
            JoinType::inner,
        );
        $root = new Collect(NodeMother::select($joinEach));
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        $plan = (new PipelineSplit())->of($logical, $planned, NodeMother::context());

        static::assertInstanceOf(Raw::class, $plan);
        static::assertNull($plan->schema);
        static::assertSame(
            [JoinEachRowsTransformer::class, SelectEntriesTransformer::class, CollectingProcessor::class],
            PipelineSteps::classes($plan->root()->segments()),
        );
        static::assertSame($planned->of($root)->steps[0], PipelineSteps::of($plan->root()->segments())[2]);
        static::assertSame($planned->of($joinEach)->steps[0], PipelineSteps::of($plan->root()->segments())[0]);
    }

    public function test_a_refusal_inside_a_sink_keeps_the_schema_of_the_returned_rows(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $joinEach = new JoinEach(
            $read,
            new StaticDataFrameFactory(df()->read(from_array([['id' => 1]], schema(int_schema('id'))))),
            join_on(['id' => 'id']),
            JoinType::inner,
        );
        $logical = Trigger::rows->plan($read, new Sinks(new Write($joinEach, to_memory(new ArrayMemory()))));
        $planned = new PlannedNodes();
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        $plan = (new PipelineSplit())->of($logical, $planned, NodeMother::context());

        static::assertInstanceOf(Raw::class, $plan);
        static::assertEquals(schema(int_schema('id')), $plan->schema());
    }

    public function test_a_bound_plan_uses_bound_steps_for_every_node(): void
    {
        $root = new Collect(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))));
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        $plan = (new PipelineSplit())->of($logical, $planned, NodeMother::context());

        static::assertSame($planned->of($root)->bound[0], PipelineSteps::of($plan->root()->segments())[0]);
        static::assertNotSame($planned->of($root)->steps[0], PipelineSteps::of($plan->root()->segments())[0]);
    }

    public function test_a_spine_that_does_not_end_in_a_read_is_refused(): void
    {
        $leaf = new ChildlessNode();
        $planned = new PlannedNodes();
        $planned->add($leaf, new PlannedNode([], [], null));

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A logical plan must end in a Read, ' . $leaf::class . ' found');

        (new PipelineSplit())->of(new LogicalPlan(new Result($leaf)), $planned, NodeMother::context());
    }

    public function test_a_joins_right_side_stays_off_the_spine(): void
    {
        $left = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $root = new Node\CrossJoin(
            $left,
            NodeMother::joinRight(NodeMother::plan(NodeMother::read(from_array([[
                'id' => 1,
            ]], schema(int_schema('id')))))),
            'r_',
        );
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        $plan = (new PipelineSplit())->of($logical, $planned, NodeMother::context());

        static::assertNull($plan->root()->input());
        static::assertSame($left->extractor(), $plan->root()->segments()->extractor());
        static::assertSame([CrossJoinRowsTransformer::class], PipelineSteps::classes($plan->root()->segments()));
    }

    public function test_a_sink_of_an_outputs_below_the_root_is_attached_to_the_spine(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $loader = to_memory(new ArrayMemory());
        $logical = NodeMother::plan(NodeMother::limit(new Outputs(new Result($read), new Write($read, $loader)), 5));
        $planned = new PlannedNodes();
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        $plan = (new PipelineSplit())->of($logical, $planned, NodeMother::context());

        static::assertContains($loader, PipelineSteps::of($plan->root()->segments()));
        static::assertNull($plan->root()->input());
    }

    public function test_a_bare_sink_at_the_spine_root_is_a_step_of_the_spine(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $loader = to_memory(new ArrayMemory());

        $plan = (new Planner())->plan(
            new LogicalPlan(new Outputs(new Result($read), new Write($read, $loader))),
            NodeMother::context(),
        );

        static::assertSame([$loader], PipelineSteps::of($plan->root()->segments()));
        static::assertSame(0, $plan->root()->id);
    }

    public function test_a_write_root_puts_its_loader_on_the_spine_exactly_once(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $loader = to_memory(new ArrayMemory());

        $plan = (new Planner())->plan(
            Trigger::run->plan($read, new Sinks(new Write($read, $loader))),
            NodeMother::context(),
        );

        static::assertSame([$loader], PipelineSteps::of($plan->root()->segments()));
    }

    public function test_two_writes_under_run_are_attached_in_the_order_the_root_lists_them(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $first = to_memory(new ArrayMemory());
        $second = to_memory(new ArrayMemory());

        $plan = (new Planner())->plan(
            Trigger::run->plan($read, new Sinks(new Write($read, $first), new Write($read, $second))),
            NodeMother::context(),
        );

        static::assertSame([$first, $second], PipelineSteps::of($plan->root()->segments()));
    }

    public function test_a_bare_sink_at_a_processor_heads_the_next_segment(): void
    {
        $batch = new Node\Batch(
            NodeMother::select(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
            10,
        );

        $segments = (new Planner())
            ->plan(
                new LogicalPlan(
                    new Outputs(
                        new Result(NodeMother::select($batch)),
                        new Write($batch, to_memory(new ArrayMemory())),
                    ),
                ),
                NodeMother::context(),
            )
            ->root()
            ->segments()
            ->all();

        static::assertCount(2, $segments);
        static::assertInstanceOf(BatchingProcessor::class, $segments[0]->processor());
        static::assertSame(
            [MemoryLoader::class, SelectEntriesTransformer::class],
            array_map(static fn($step) => $step::class, $segments[1]->steps()),
        );
    }

    public function test_a_bare_sink_at_a_blocking_node_below_the_root_lives_in_the_pipeline_the_cut_closes(): void
    {
        $collect = new Collect(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))));

        $plan = (new Planner())->plan(
            new LogicalPlan(
                new Outputs(
                    new Result(NodeMother::limit($collect, 5)),
                    new Write($collect, to_memory(new ArrayMemory())),
                ),
            ),
            NodeMother::context(),
        );

        $upstream = $plan->root()->input()?->segments()->all() ?? [];
        static::assertCount(2, $upstream);
        static::assertInstanceOf(CollectingProcessor::class, $upstream[0]->processor());
        static::assertSame([MemoryLoader::class], array_map(static fn($step) => $step::class, $upstream[1]->steps()));
        static::assertSame([LimitTransformer::class], PipelineSteps::classes($plan->root()->segments()));
    }

    public function test_a_sink_with_its_own_steps_is_fed_through_one_side_pipeline(): void
    {
        $memory = new ArrayMemory();
        $loader = to_memory($memory);
        $read = NodeMother::read(from_array(
            [['id' => 1, 'name' => 'a']],
            schema(int_schema('id'), str_schema('name')),
        ));

        $plan = (new Planner())->plan(
            new LogicalPlan(new Outputs(new Result($read), new Write(NodeMother::select($read), $loader))),
            NodeMother::context(),
        );

        $steps = PipelineSteps::of($plan->root()->segments());
        static::assertCount(1, $steps);
        static::assertInstanceOf(SinkFeed::class, $steps[0]);
        static::assertSame(1, $plan->root()->id);

        foreach ((new Executor())->executePipeline($plan->root()) as $_) {
        }

        static::assertSame([['id' => 1]], $memory->dump());
    }

    public function test_a_node_two_sinks_share_off_the_spine_runs_once_in_one_side_pipeline(): void
    {
        $first = new ArrayMemory();
        $second = new ArrayMemory();
        $spy = new SpyTransformer();
        $read = NodeMother::read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $shared = new Node\Transform($read, $spy);

        $plan = (new Planner())->plan(
            new LogicalPlan(
                new Outputs(
                    new Result($read),
                    new Write($shared, to_memory($first)),
                    new Write($shared, to_memory($second)),
                ),
            ),
            NodeMother::context(),
        );

        static::assertSame([SinkFeed::class], PipelineSteps::classes($plan->root()->segments()));

        foreach ((new Executor())->executePipeline($plan->root()) as $_) {
        }

        static::assertSame(2, $spy->seen);
        static::assertSame([['id' => 1], ['id' => 2]], $first->dump());
        static::assertSame([['id' => 1], ['id' => 2]], $second->dump());
    }

    public function test_a_transaction_whose_children_share_a_node_opens_inside_the_shared_side_pipeline(): void
    {
        $spy = new SpyTransformer();
        $transaction = new RecordingTransaction();
        $read = NodeMother::read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $shared = new Node\Transform($read, $spy);

        $plan = (new Planner())->plan(
            new LogicalPlan(
                new Outputs(
                    new Result($read),
                    new Node\Transaction(
                        $transaction,
                        new Write($shared, to_memory(new ArrayMemory())),
                        new Write($shared, to_memory(new ArrayMemory())),
                    ),
                ),
            ),
            NodeMother::context(),
        );

        static::assertSame([SinkFeed::class], PipelineSteps::classes($plan->root()->segments()));

        foreach ((new Executor())->executePipeline($plan->root()) as $_) {
        }

        static::assertSame(2, $spy->seen);
        static::assertSame(['begin', 'commit', 'begin', 'commit'], $transaction->log);
    }

    public function test_transaction_children_sharing_a_node_among_some_siblings_are_fed_through_one_group_pipeline(): void
    {
        $shared = new ArrayMemory();
        $bare = new ArrayMemory();
        $spy = new SpyTransformer();
        $read = NodeMother::read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $transform = new Node\Transform($read, $spy);

        $plan = (new Planner())->plan(
            new LogicalPlan(
                new Outputs(
                    new Result($read),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($transform, to_memory($shared)),
                        new Write($transform, to_memory(new ArrayMemory())),
                        new Write($read, to_memory($bare)),
                    ),
                ),
            ),
            NodeMother::context(),
        );

        static::assertSame([TransactionalSinks::class], PipelineSteps::classes($plan->root()->segments()));

        foreach ((new Executor())->executePipeline($plan->root()) as $_) {
        }

        static::assertSame(2, $spy->seen);
        static::assertSame([['id' => 1], ['id' => 2]], $shared->dump());
        static::assertSame([['id' => 1], ['id' => 2]], $bare->dump());
    }

    public function test_a_sink_outside_a_transaction_sharing_a_node_with_one_of_its_children_is_refused(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $shared = NodeMother::select($read);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A sink outside a transaction cannot share a node with one of its children');

        (new Planner())->plan(
            new LogicalPlan(
                new Outputs(
                    new Result($read),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($shared, to_memory(new ArrayMemory())),
                        new Write($read, to_memory(new ArrayMemory())),
                    ),
                    new Write($shared, to_memory(new ArrayMemory())),
                ),
            ),
            NodeMother::context(),
        );
    }

    public function test_a_transaction_is_one_step_over_its_children(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));

        $plan = (new Planner())->plan(
            new LogicalPlan(
                new Outputs(
                    new Result($read),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($read, to_memory(new ArrayMemory())),
                        new Write(NodeMother::select($read), to_memory(new ArrayMemory())),
                    ),
                ),
            ),
            NodeMother::context(),
        );

        static::assertSame([TransactionalSinks::class], PipelineSteps::classes($plan->root()->segments()));
        static::assertSame(1, $plan->root()->id);
    }

    public function test_two_sinks_at_one_node_are_two_steps(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $first = to_memory(new ArrayMemory());
        $second = to_memory(new ArrayMemory());

        $plan = (new Planner())->plan(
            new LogicalPlan(new Outputs(new Result($read), new Write($read, $first), new Write($read, $second))),
            NodeMother::context(),
        );

        static::assertSame([$first, $second], PipelineSteps::of($plan->root()->segments()));
    }

    public function test_a_sink_sharing_no_node_with_the_spine_is_refused(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A sink root shares no node with the plan: ' . MemoryLoader::class);

        (new Planner())->plan(
            new LogicalPlan(
                new Outputs(
                    new Result(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
                    new Write(
                        NodeMother::select(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
                        to_memory(new ArrayMemory()),
                    ),
                ),
            ),
            NodeMother::context(),
        );
    }

    public function test_the_children_of_one_transaction_must_attach_to_the_same_node(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $select = NodeMother::select($read);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('Every sink of one transaction must attach to the same node');

        (new Planner())->plan(
            new LogicalPlan(
                new Outputs(
                    new Result($select),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($read, to_memory(new ArrayMemory())),
                        new Write($select, to_memory(new ArrayMemory())),
                    ),
                ),
            ),
            NodeMother::context(),
        );
    }

    public function test_a_plan_with_an_undescribable_op_still_attaches_its_sinks(): void
    {
        $joinEach = new JoinEach(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            new StaticDataFrameFactory(df()->read(from_array([['id' => 1]], schema(int_schema('id'))))),
            join_on(['id' => 'id']),
            JoinType::inner,
        );
        $select = NodeMother::select($joinEach);

        $plan = (new Planner())->plan(
            new LogicalPlan(
                new Outputs(
                    new Result(new Collect($select)),
                    new Write($select, to_memory(new ArrayMemory())),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write(NodeMother::select($select), to_memory(new ArrayMemory())),
                    ),
                ),
            ),
            NodeMother::context(),
        );

        static::assertInstanceOf(Raw::class, $plan);
        static::assertSame(
            [
                JoinEachRowsTransformer::class,
                SelectEntriesTransformer::class,
                MemoryLoader::class,
                TransactionalSinks::class,
                CollectingProcessor::class,
            ],
            PipelineSteps::classes($plan->root()->segments()),
        );
    }

    public function test_a_write_that_does_not_translate_to_a_loader_is_refused(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $write = new Write($read, to_memory(new ArrayMemory()));
        $logical = new LogicalPlan(new Outputs(new Result($read), $write));
        $planned = new PlannedNodes();
        (new Planner())->node($logical->root, NodeMother::context(), $planned);
        $planned->add($write, new PlannedNode([], [], null));

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A Write must translate to a Loader, null given');

        (new PipelineSplit())->of($logical, $planned, NodeMother::context());
    }

    public function test_a_batched_transaction_sink_is_one_step_behind_the_batch_by_processor(): void
    {
        $batchBy = new Node\BatchBy(
            NodeMother::read(from_array([['transaction_id' => 7]], schema(int_schema('transaction_id')))),
            ref('transaction_id'),
            5_000,
        );

        $plan = (new Planner())->plan(
            new LogicalPlan(
                new Outputs(
                    new Result($batchBy),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write(NodeMother::select($batchBy, 'transaction_id'), to_memory(new ArrayMemory())),
                        new Write(new Node\Batch($batchBy, 1_000), to_memory(new ArrayMemory())),
                    ),
                ),
            ),
            NodeMother::context(),
        );

        $segments = $plan->root()->segments()->all();
        static::assertNull($plan->root()->input());
        static::assertSame(2, $plan->root()->id);
        static::assertCount(2, $segments);
        static::assertSame([], $segments[0]->steps());
        static::assertInstanceOf(BatchingByProcessor::class, $segments[0]->processor());
        static::assertSame(
            [TransactionalSinks::class],
            array_map(static fn($step) => $step::class, $segments[1]->steps()),
        );
    }

    public function test_the_leaf_pipeline_carries_the_reads_limit_and_path_filter(): void
    {
        $root = NodeMother::limit(
            NodeMother::sort(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))->withLimit(5)),
            5,
        );
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        $plan = (new PipelineSplit())->of($logical, $planned, NodeMother::context());

        static::assertSame(5, $plan->root()->input()?->limit());
        static::assertNull($plan->root()->limit());
    }

    public function test_only_the_pipeline_that_reads_the_source_counts_its_rows(): void
    {
        $logical = new LogicalPlan(new Result(NodeMother::limit(
            NodeMother::sort(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
            5,
        )));
        $planned = new PlannedNodes();
        $sources = new SourceRows();
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        $plan = (new PipelineSplit())->of($logical, $planned, NodeMother::context(), $sources);

        static::assertSame($sources, $plan->root()->input()?->sources());
        static::assertNull($plan->root()->sources());
    }

    public function test_a_pipeline_counts_no_rows_unless_asked(): void
    {
        $logical = new LogicalPlan(new Result(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))));
        $planned = new PlannedNodes();
        (new Planner())->node($logical->root, NodeMother::context(), $planned);

        static::assertNull(
            (new PipelineSplit())
                ->of($logical, $planned, NodeMother::context())
                ->root()
                ->sources(),
        );
    }
}
