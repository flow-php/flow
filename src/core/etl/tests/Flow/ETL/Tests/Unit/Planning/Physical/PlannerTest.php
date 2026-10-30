<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Physical;

use ArrayObject;
use Flow\ETL\Foundation\Exception\DataDependentSchemaException;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\IO\Floe\FloeLoader;
use Flow\ETL\IO\Loader\ArrayLoader;
use Flow\ETL\IO\Loader\MemoryLoader;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\Join\CrossJoin as CrossJoinOperator;
use Flow\ETL\Operators\Join\HashJoin;
use Flow\ETL\Operators\Processor\BatchingByProcessor;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Operators\Processor\CountingProcessor;
use Flow\ETL\Operators\Processor\DistinctProcessor;
use Flow\ETL\Operators\Processor\GroupByAggregationProcessor;
use Flow\ETL\Operators\Processor\MemorySortProcessor;
use Flow\ETL\Operators\Processor\OffsetProcessor;
use Flow\ETL\Operators\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Operators\Transformer\SelectColumnsTransformer;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Aggregate;
use Flow\ETL\Planning\Logical\Node\Collect;
use Flow\ETL\Planning\Logical\Node\CrossJoin;
use Flow\ETL\Planning\Logical\Node\JoinEach;
use Flow\ETL\Planning\Logical\Node\Limit;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Select;
use Flow\ETL\Planning\Logical\Node\Sort;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Logical\Trigger;
use Flow\ETL\Planning\Logical\Writes;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Planning\Optimizer\Rule\CombineSortAndLimit;
use Flow\ETL\Planning\Physical\BranchPipeline;
use Flow\ETL\Planning\Physical\JoinStep;
use Flow\ETL\Planning\Physical\Pipeline;
use Flow\ETL\Planning\Physical\PipelineSteps;
use Flow\ETL\Planning\Physical\PlannedNode;
use Flow\ETL\Planning\Physical\PlannedNodes;
use Flow\ETL\Planning\Physical\Planner;
use Flow\ETL\Planning\Physical\TransactionGroup;
use Flow\ETL\Runtime\Executor\Executor;
use Flow\ETL\Runtime\Executor\SourceCounts;
use Flow\ETL\Runtime\Metrics\SourceStatistics;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\Context\PipelineStepList;
use Flow\ETL\Tests\Double\BackendRecordingTransformer;
use Flow\ETL\Tests\Double\ChildlessNode;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\RecordingRule;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\SpineCopyingRule;
use Flow\ETL\Tests\Double\SpyTransformer;
use Flow\ETL\Tests\Double\StaticExtractorFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\HashJoinMother;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;
use Flow\ETL\Tests\Mother\PipelinesMother;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Join\JoinType;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;
use function Flow\ETL\DSL\to_memory;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\to_floe;
use function Flow\Types\DSL\type_instance_of;
use function iterator_to_array;

final class PlannerTest extends FlowTestCase
{
    public function test_the_default_rules_keep_every_write_on_the_spine(): void
    {
        $read = NodeMother::read();
        $three = NodeMother::limit(NodeMother::limit($read, 5), 3);
        $loader = to_memory(new ArrayMemory());

        $plan = config()
            ->planner()
            ->plan(new LogicalPlan(
                new Outputs(
                    new Result(NodeMother::select($three)),
                    new Write($three, $loader),
                    new Write(NodeMother::limit($read, 4), to_memory(new ArrayMemory())),
                ),
            ));

        $steps = PipelineStepList::of($plan->root());
        static::assertSame(
            4,
            type_instance_of(Read::class)->assert($plan->root()->chain()[0]->input)->criteria()->limit,
        );
        static::assertSame(
            [BranchPipeline::class, LimitTransformer::class, MemoryLoader::class, SelectColumnsTransformer::class],
            array_map(static fn($step) => $step::class, $steps),
        );
        static::assertSame($loader, $steps[2]);
    }

    public function test_a_rule_that_rebuilds_the_plan_around_transform_up_keeps_its_writes(): void
    {
        $select = NodeMother::select(NodeMother::read());

        // both writes are children of the one root, so a rule copying the root cannot lose them
        static::assertSame(
            [SelectColumnsTransformer::class, MemoryLoader::class, BranchPipeline::class],
            PipelineStepList::classes(
                config_builder()
                    ->optimizer(new Optimizer(new SpineCopyingRule()))
                    ->build()
                    ->planner()
                    ->plan(new LogicalPlan(
                        new Outputs(
                            new Result($select),
                            new Write($select, to_memory(new ArrayMemory())),
                            new Write(NodeMother::limit($select, 1), to_memory(new ArrayMemory())),
                        ),
                    ))
                    ->root(),
            ),
        );
    }

    public function test_a_linear_plan_is_one_pipeline_with_the_limit_pushed(): void
    {
        $memory = memory_filesystem();
        $orders = FloeFilesContext::extractor($memory, 'memory://planner/orders.floe', RowsMother::orders());
        $node = new Read($orders);
        $node = new Select($node, ['id', 'total']);
        $node = new Limit($node, 5);
        $node = new Write($node, to_floe(path('memory://planner/out.floe'), filesystem: $memory));

        $logical = new LogicalPlan(new Result($node));
        $plan = config()->planner()->plan($logical);

        static::assertSame(0, $plan->root()->id);
        static::assertSame(
            [SelectColumnsTransformer::class, LimitTransformer::class, FloeLoader::class],
            PipelineStepList::classes($plan->root()),
        );
        static::assertNull($plan->root()->input());
        static::assertTrue($plan->described());
        static::assertSame(['id', 'total'], $plan->schema()->names());

        static::assertSame(
            $orders,
            type_instance_of(Read::class)->assert($plan->root()->chain()[0]->input)->extractor(),
        );
        static::assertSame(
            5,
            type_instance_of(Read::class)->assert($plan->root()->chain()[0]->input)->criteria()->limit,
        );
        static::assertNull($logical->source()->criteria()->limit);
    }

    public function test_a_run_plan_over_one_write_is_described_with_the_writes_input_schema(): void
    {
        $memory = memory_filesystem();
        $plan = config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(Trigger::run->plan(
                $select = new Select(
                    new Read(FloeFilesContext::extractor(
                        $memory,
                        'memory://planner/orders.floe',
                        RowsMother::orders(),
                    )),
                    ['id'],
                ),
                new Writes(new Write($select, to_floe(path('memory://planner/out.floe'), filesystem: $memory))),
            ));

        static::assertTrue($plan->described());
        static::assertSame(['id'], $plan->schema()->names());
    }

    public function test_a_sort_splits_the_plan_into_two_pipelines(): void
    {
        $memory = memory_filesystem();
        $orders = FloeFilesContext::extractor($memory, 'memory://planner/orders.floe', RowsMother::orders());
        $node = new Read($orders);
        $node = new Sort($node, [ref('total')->asc()], memory_sort());
        $node = new Limit($node, 5);
        $node = new Write($node, to_floe(path('memory://planner/out.floe'), filesystem: $memory));

        $plan = config_builder()
            ->optimizer(Optimizer::default()->without(CombineSortAndLimit::class))
            ->build()
            ->planner()
            ->plan(new LogicalPlan(new Result($node)));

        static::assertSame(1, $plan->root()->id);
        static::assertSame([LimitTransformer::class, FloeLoader::class], PipelineStepList::classes($plan->root()));
        static::assertInstanceOf(Pipeline::class, $plan->root()->input);

        $upstream = $plan->root()->input();

        static::assertNotNull($upstream);
        static::assertSame(0, $upstream->id);
        static::assertSame([MemorySortProcessor::class], PipelineStepList::classes($upstream));
        static::assertTrue($plan->described());
        static::assertSame(['id', 'total', 'seller_id'], $plan->schema()->names());

        static::assertSame($orders, type_instance_of(Read::class)->assert($upstream->chain()[0]->input)->extractor());
        static::assertNull(type_instance_of(Read::class)->assert($upstream->chain()[0]->input)->criteria()->limit);
    }

    public function test_a_join_each_plan_refuses_its_schema_and_has_no_frame_edge(): void
    {
        $memory = memory_filesystem();
        $node = new Read(FloeFilesContext::extractor($memory, 'memory://planner/orders.floe', RowsMother::orders()));
        $node = new JoinEach(
            $node,
            new StaticExtractorFactory(from_array([['id' => 1]])),
            join_on(['id' => 'id']),
            JoinType::left,
        );

        $plan = config()->planner()->plan(new LogicalPlan(new Result($node)));

        static::assertSame(0, $plan->root()->id);
        static::assertSame([JoinEachRowsTransformer::class], PipelineStepList::classes($plan->root()));
        static::assertFalse($plan->described());
        static::assertNull($plan->root()->input());
    }

    public function test_rules_run_once_each_in_registration_order(): void
    {
        $log = new ArrayObject();

        config_builder()
            ->optimizer(new Optimizer(new RecordingRule('first', $log), new RecordingRule('second', $log)))
            ->build()
            ->planner()
            ->plan(NodeMother::plan(NodeMother::read()));

        static::assertSame(['first', 'second'], $log->getArrayCopy());
    }

    public function test_a_planner_with_no_rules_plans_without_a_push(): void
    {
        $memory = memory_filesystem();
        $orders = FloeFilesContext::extractor($memory, 'memory://planner/orders.floe', RowsMother::orders());
        $node = new Read($orders);
        $node = new Limit($node, 5);

        $plan = config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(new Result($node)));

        static::assertSame(
            $orders,
            type_instance_of(Read::class)->assert($plan->root()->chain()[0]->input)->extractor(),
        );
        static::assertNull(type_instance_of(Read::class)->assert($plan->root()->chain()[0]->input)->criteria()->limit);
    }

    public function test_a_join_plans_its_right_side_as_a_physical_plan_of_its_own(): void
    {
        $memory = memory_filesystem();
        $node = new Read(FloeFilesContext::extractor($memory, 'memory://planner/orders.floe', RowsMother::orders()));
        $node = new Node\Join(
            $node,
            df()
                ->read(FloeFilesContext::extractor($memory, 'memory://planner/sellers.floe', RowsMother::sellers()))
                ->explain()
                ->logical
                ->root,
            join_on(['seller_id' => 'id'], 'r_'),
            JoinType::inner,
        );
        $node = new Write($node, to_floe(path('memory://planner/out.floe'), filesystem: $memory));

        $plan = config()->planner()->plan(new LogicalPlan(new Result($node)));

        static::assertSame(1, $plan->root()->id);
        static::assertSame([FloeLoader::class], PipelineStepList::classes($plan->root()));

        $upstream = $plan->root()->input();

        static::assertNotNull($upstream);
        static::assertSame(0, $upstream->id);
        static::assertNull($upstream->input());
        static::assertSame([HashJoin::class], PipelineStepList::classes($upstream));
        static::assertTrue($plan->described());
        static::assertSame(['id', 'total', 'seller_id', 'r_id', 'r_name'], $plan->schema()->names());
    }

    public function test_a_joins_right_side_runs_on_the_outer_backend(): void
    {
        $recording = new BackendRecordingTransformer();
        $node = new CrossJoin(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            df(config())
                ->read(from_array([['id' => 1]], schema(int_schema('id'))))
                ->with($recording)
                ->explain()
                ->logical
                ->root,
            'r_',
        );
        $context = NodeMother::context(config());

        $plan = $context->config->planner()->plan(new LogicalPlan(new Result($node)));
        iterator_to_array($context->config->executor()->executePlan($plan, $context));

        static::assertSame([$context->backend()], $recording->backends);
    }

    public function test_a_joins_right_side_counts_its_source_rows_too(): void
    {
        $node = new CrossJoin(
            NodeMother::read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id')))),
            df()
                ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id'))))
                ->explain()
                ->logical
                ->root,
            'r_',
        );
        $context = NodeMother::context(config());
        $sources = new SourceCounts();

        $plan = $context->config->planner()->plan(new LogicalPlan(new Result($node)));
        iterator_to_array($context->config->executor()->executePlan($plan, $context, $sources));

        static::assertSame(
            [2, 3],
            array_map(static fn(SourceStatistics $source): int => $source->rows, $sources->statistics()),
        );
    }

    public function test_a_nodes_schema_is_the_fold_of_its_bound_steps(): void
    {
        $planned = new PlannedNodes();
        $read = new Read(new CountingExtractor(schema(int_schema('id'), str_schema('name'))));

        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            config()->planner()->node($read, $planned)->schema,
        );
        static::assertEquals(
            schema(str_schema('name')),
            config()
                ->planner()
                ->node(NodeMother::select(NodeMother::select($read, 'id', 'name'), 'name'), $planned)
                ->schema,
        );
    }

    public function test_a_loader_passes_through_the_fold_unbound(): void
    {
        $output = [];
        $loader = to_array($output);

        $plannedNode = config()
            ->planner()
            ->node(
                new Write(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))), $loader),
                new PlannedNodes(),
            );

        static::assertSame([$loader], $plannedNode->steps);
        static::assertSame([$loader], $plannedNode->bound);
        static::assertEquals(schema(int_schema('id')), $plannedNode->schema);
    }

    public function test_bound_steps_are_the_result_of_bind_not_the_translated_instances(): void
    {
        $plannedNode = config()
            ->planner()
            ->node(
                new Node\Collect(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
                new PlannedNodes(),
            );

        static::assertInstanceOf(CollectingProcessor::class, $plannedNode->steps[0]);
        static::assertInstanceOf(CollectingProcessor::class, $plannedNode->bound[0]);
        static::assertNotSame($plannedNode->steps[0], $plannedNode->bound[0]);
    }

    public function test_the_same_node_object_is_planned_once(): void
    {
        $planned = new PlannedNodes();
        $node = NodeMother::select(NodeMother::read());

        static::assertSame(config()->planner()->node($node, $planned), config()->planner()->node($node, $planned));
    }

    public function test_a_joins_right_side_is_planned_apart_from_this_plan(): void
    {
        $planned = new PlannedNodes();
        $shared = NodeMother::select(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))));
        $frame = NodeMother::joinRight(NodeMother::plan($shared));

        config()->planner()->node(NodeMother::crossJoin($shared, $frame), $planned);

        static::assertTrue($planned->has($shared));
        static::assertFalse($planned->has($frame));
    }

    public function test_a_refusal_anywhere_makes_the_whole_plan_raw(): void
    {
        $planned = new PlannedNodes();
        $joinEach = new JoinEach(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            new StaticExtractorFactory(from_array([['id' => 1]], schema(int_schema('id')))),
            join_on(['id' => 'id']),
            JoinType::inner,
        );
        $above = NodeMother::select($joinEach);

        config()->planner()->node($above, $planned);

        $refusal = $planned->refusal();

        static::assertNotNull($refusal);
        static::assertSame(DataDependentSchemaException::class, $refusal::class);
        static::assertStringContainsString('JoinEachRowsTransformer', $refusal->getMessage());
        static::assertStringContainsString(
            "its right side is built from each left batch's row values",
            $refusal->getMessage(),
        );
        static::assertNull($planned->of($joinEach)->schema);
        static::assertSame($planned->of($joinEach)->steps, $planned->of($joinEach)->bound);
        $abovePlanned = $planned->of($above);

        static::assertNull($abovePlanned->schema);
        static::assertCount(1, $abovePlanned->steps);
    }

    public function test_a_frame_whose_write_refuses_still_describes_its_rows(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $frame = NodeMother::joinRight(Trigger::rows->plan(
            $read,
            new Writes(
                new Write(
                    new JoinEach(
                        $read,
                        new StaticExtractorFactory(from_array([['id' => 1]], schema(int_schema('id')))),
                        join_on(['id' => 'id']),
                        JoinType::inner,
                    ),
                    to_memory(new ArrayMemory()),
                ),
            ),
        ));
        $planned = new PlannedNodes();

        static::assertEquals(schema(int_schema('id')), config()->planner()->node($frame, $planned)->schema);
        static::assertNotNull($planned->refusal());
    }

    public function test_the_first_refusal_is_kept(): void
    {
        $planned = new PlannedNodes();
        $factory = new StaticExtractorFactory(from_array([['id' => 1]], schema(int_schema('id'))));

        config()
            ->planner()
            ->node(
                new JoinEach(
                    new JoinEach(
                        NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
                        $factory,
                        join_on(['id' => 'id']),
                        JoinType::inner,
                    ),
                    $factory,
                    join_on(['id' => 'id']),
                    JoinType::left,
                ),
                $planned,
            );

        $refusal = $planned->refusal();

        static::assertNotNull($refusal);
        static::assertSame(DataDependentSchemaException::class, $refusal::class);
        static::assertStringContainsString('JoinEachRowsTransformer', $refusal->getMessage());
        static::assertSame($refusal, $planned->refusal());
    }

    public function test_a_node_never_walked_is_not_planned(): void
    {
        $planned = new PlannedNodes();
        config()->planner()->node(NodeMother::read(), $planned);

        static::assertFalse($planned->has(NodeMother::read()));
    }

    public function test_a_planned_node_with_a_null_schema_is_distinct_from_a_node_never_walked(): void
    {
        $planned = new PlannedNodes();
        $refusing = new JoinEach(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            new StaticExtractorFactory(from_array([['id' => 1]], schema(int_schema('id')))),
            join_on(['id' => 'id']),
            JoinType::inner,
        );
        config()->planner()->node($refusing, $planned);

        static::assertTrue($planned->has($refusing));
        static::assertNull($planned->of($refusing)->schema);
        static::assertFalse($planned->has(NodeMother::read()));
    }

    public function test_planning_reads_no_row(): void
    {
        $extractor = new CountingExtractor(
            schema(int_schema('id')),
            array_to_rows([['id' => 1]], schema(int_schema('id'))),
        );

        config()->planner()->node(NodeMother::select(new Read($extractor)), new PlannedNodes());

        static::assertSame(0, $extractor->extractCalls);
    }

    public function test_a_joins_right_side_is_handed_to_the_translation_as_a_physical_plan(): void
    {
        static::assertInstanceOf(
            JoinStep::class,
            config()
                ->planner()
                ->node(
                    new CrossJoin(
                        NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
                        NodeMother::joinRight(NodeMother::plan(NodeMother::read(from_array([[
                            'id' => 1,
                        ]], schema(int_schema('id')))))),
                        'r_',
                    ),
                    new PlannedNodes(),
                )
                ->steps[0],
        );
    }

    public function test_a_per_operation_algorithm_is_the_config_its_processor_holds(): void
    {
        $root = new Collect(
            new Aggregate(
                NodeMother::read(PipelinesMother::source()),
                new GroupBy(refs('id'), new Aggregators(count(ref('id')))),
                hash_group_by()->storage(new MemoryRowsStorage())->bucketsCount(9),
            ),
        );
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        config()->planner()->node($logical->root, $planned);

        static::assertSame(
            9,
            type_instance_of(GroupByAggregationProcessor::class)
                ->assert(config()->planner()->pipelines($logical, $planned)->pipelines()[0]->steps->breaker)
                ->config
                ->bucketsCount,
        );
    }

    public function test_a_plan_ending_in_a_breaker_stays_one_pipeline(): void
    {
        $root = new Collect(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))));
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        config()->planner()->node($logical->root, $planned);

        $plan = config()->planner()->pipelines($logical, $planned);

        static::assertSame(0, $plan->root()->id);
        static::assertNull($plan->root()->input());
        static::assertSame([CollectingProcessor::class], PipelineStepList::classes($plan->root()));
    }

    public function test_count_as_the_top_node_leaves_no_empty_pipeline(): void
    {
        $root = new Node\Count(NodeMother::sort(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))));
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        config()->planner()->node($logical->root, $planned);

        $pipelines = config()->planner()->pipelines($logical, $planned)->pipelines();

        static::assertCount(2, $pipelines);
        static::assertSame([CountingProcessor::class], PipelineStepList::classes($pipelines[1]));
    }

    public function test_ids_count_up_from_the_leaf_and_the_root_has_the_highest(): void
    {
        $root = NodeMother::sort(
            new Collect(NodeMother::sort(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))))),
            [ref('id')->asc()],
        );
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        config()->planner()->node($logical->root, $planned);

        $plan = config()->planner()->pipelines($logical, $planned);

        static::assertSame(2, $plan->root()->id);
        static::assertSame(1, $plan->root()->input()?->id);
        static::assertSame(0, $plan->root()->input()?->input()?->id);
        static::assertNull($plan->root()->input()?->input()?->input());
    }

    public function test_a_step_after_a_breaker_opens_the_next_pipeline(): void
    {
        $root = NodeMother::limit(
            new Node\Sort(
                NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
                [ref('id')->asc()],
                memory_sort(),
            ),
            5,
        );
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        config()->planner()->node($logical->root, $planned);

        $pipelines = config()->planner()->pipelines($logical, $planned)->pipelines();

        static::assertCount(2, $pipelines);
        static::assertSame([], $pipelines[0]->steps->steps);
        static::assertInstanceOf(MemorySortProcessor::class, $pipelines[0]->steps->breaker);
        static::assertInstanceOf(Pipeline::class, $pipelines[1]->input);
        static::assertSame([LimitTransformer::class], PipelineStepList::classes($pipelines[1]));
    }

    public function test_a_streaming_processor_does_not_cut(): void
    {
        $output = [];
        $root = new Write(
            NodeMother::select(
                new Node\Distinct(
                    new Node\Offset(
                        new Node\Batch(
                            NodeMother::select(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
                            new BatchSize(10),
                        ),
                        1,
                    ),
                    ['id'],
                ),
            ),
            to_array($output),
        );
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        config()->planner()->node($logical->root, $planned);

        $plan = config()->planner()->pipelines($logical, $planned);

        static::assertCount(1, $plan->pipelines());
        static::assertNull($plan->root()->steps->breaker);
        static::assertSame(
            [
                SelectColumnsTransformer::class,
                BatchingProcessor::class,
                OffsetProcessor::class,
                DistinctProcessor::class,
                SelectColumnsTransformer::class,
                ArrayLoader::class,
            ],
            PipelineStepList::classes($plan->root()),
        );
    }

    public function test_a_refused_plan_uses_raw_steps_for_every_node(): void
    {
        $joinEach = new JoinEach(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            new StaticExtractorFactory(from_array([['id' => 1]], schema(int_schema('id')))),
            join_on(['id' => 'id']),
            JoinType::inner,
        );
        $root = new Collect(NodeMother::select($joinEach));
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        config()->planner()->node($logical->root, $planned);

        $plan = config()->planner()->pipelines($logical, $planned);

        static::assertFalse($plan->described());
        static::assertNotNull($plan->refusal());
        static::assertSame(
            [JoinEachRowsTransformer::class, SelectColumnsTransformer::class, CollectingProcessor::class],
            PipelineStepList::classes($plan->root()),
        );
        static::assertSame($planned->of($root)->steps[0], PipelineStepList::of($plan->root())[2]);
        static::assertSame($planned->of($joinEach)->steps[0], PipelineStepList::of($plan->root())[0]);
    }

    public function test_a_refusal_inside_a_write_keeps_the_schema_of_the_returned_rows(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $logical = Trigger::rows->plan(
            $read,
            new Writes(
                new Write(
                    new JoinEach(
                        $read,
                        new StaticExtractorFactory(from_array([['id' => 1]], schema(int_schema('id')))),
                        join_on(['id' => 'id']),
                        JoinType::inner,
                    ),
                    to_memory(new ArrayMemory()),
                ),
            ),
        );
        $planned = new PlannedNodes();
        config()->planner()->node($logical->root, $planned);

        $plan = config()->planner()->pipelines($logical, $planned);

        static::assertFalse($plan->described());
        static::assertEquals(schema(int_schema('id')), $plan->schema());
    }

    public function test_a_bound_plan_uses_bound_steps_for_every_node(): void
    {
        $root = new Collect(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))));
        $planned = new PlannedNodes();
        $logical = new LogicalPlan(new Result($root));
        config()->planner()->node($logical->root, $planned);

        $plan = config()->planner()->pipelines($logical, $planned);

        static::assertSame($planned->of($root)->bound[0], PipelineStepList::of($plan->root())[0]);
        static::assertNotSame($planned->of($root)->steps[0], PipelineStepList::of($plan->root())[0]);
    }

    public function test_a_spine_that_does_not_end_in_a_read_is_refused(): void
    {
        $leaf = new ChildlessNode();
        $planned = new PlannedNodes();
        $planned->add($leaf, new PlannedNode([], [], null));

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A logical plan must end in a Read, ' . $leaf::class . ' found');

        config()->planner()->pipelines(new LogicalPlan(new Result($leaf)), $planned);
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
        config()->planner()->node($logical->root, $planned);

        $plan = config()->planner()->pipelines($logical, $planned);

        static::assertNull($plan->root()->input());
        static::assertSame(
            $left->extractor(),
            type_instance_of(Read::class)->assert($plan->root()->chain()[0]->input)->extractor(),
        );
        static::assertSame([CrossJoinOperator::class], PipelineStepList::classes($plan->root()));
    }

    public function test_a_write_of_an_outputs_below_the_root_is_attached_to_the_spine(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $loader = to_memory(new ArrayMemory());
        $logical = NodeMother::plan(NodeMother::limit(new Outputs(new Result($read), new Write($read, $loader)), 5));
        $planned = new PlannedNodes();
        config()->planner()->node($logical->root, $planned);

        $plan = config()->planner()->pipelines($logical, $planned);

        static::assertContains($loader, PipelineStepList::of($plan->root()));
        static::assertNull($plan->root()->input());
    }

    public function test_a_bare_write_at_the_spine_root_is_a_step_of_the_spine(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $loader = to_memory(new ArrayMemory());

        $plan = config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(new Outputs(new Result($read), new Write($read, $loader))));

        static::assertSame([$loader], PipelineStepList::of($plan->root()));
        static::assertSame(0, $plan->root()->id);
    }

    public function test_a_write_root_puts_its_loader_on_the_spine_exactly_once(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $loader = to_memory(new ArrayMemory());

        static::assertSame(
            [$loader],
            PipelineStepList::of(
                config_builder()
                    ->optimizer(new Optimizer())
                    ->build()
                    ->planner()
                    ->plan(Trigger::run->plan($read, new Writes(new Write($read, $loader))))
                    ->root(),
            ),
        );
    }

    public function test_two_writes_under_run_are_attached_in_the_order_the_root_lists_them(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $first = to_memory(new ArrayMemory());
        $second = to_memory(new ArrayMemory());

        static::assertSame(
            [$first, $second],
            PipelineStepList::of(
                config_builder()
                    ->optimizer(new Optimizer())
                    ->build()
                    ->planner()
                    ->plan(Trigger::run->plan($read, new Writes(new Write($read, $first), new Write($read, $second))))
                    ->root(),
            ),
        );
    }

    public function test_a_bare_write_at_a_streaming_processor_follows_it_in_the_pipeline(): void
    {
        $batch = new Node\Batch(
            NodeMother::select(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
            new BatchSize(10),
        );

        static::assertSame(
            [
                SelectColumnsTransformer::class,
                BatchingProcessor::class,
                MemoryLoader::class,
                SelectColumnsTransformer::class,
            ],
            PipelineStepList::classes(
                config_builder()
                    ->optimizer(new Optimizer())
                    ->build()
                    ->planner()
                    ->plan(new LogicalPlan(
                        new Outputs(
                            new Result(NodeMother::select($batch)),
                            new Write($batch, to_memory(new ArrayMemory())),
                        ),
                    ))
                    ->root(),
            ),
        );
    }

    public function test_a_bare_write_at_a_breaker_below_the_root_heads_the_next_pipeline(): void
    {
        $collect = new Collect(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))));

        [$leaf, $root] = config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(
                new Outputs(
                    new Result(NodeMother::limit($collect, 5)),
                    new Write($collect, to_memory(new ArrayMemory())),
                ),
            ))
            ->pipelines();
        static::assertSame([CollectingProcessor::class], PipelineStepList::classes($leaf));
        static::assertSame([MemoryLoader::class, LimitTransformer::class], PipelineStepList::classes($root));
    }

    public function test_a_write_with_its_own_steps_is_fed_through_one_side_pipeline(): void
    {
        $memory = new ArrayMemory();
        $loader = to_memory($memory);
        $read = NodeMother::read(from_array(
            [['id' => 1, 'name' => 'a']],
            schema(int_schema('id'), str_schema('name')),
        ));

        $plan = config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(new Outputs(new Result($read), new Write(NodeMother::select($read), $loader))));

        $steps = PipelineStepList::of($plan->root());
        static::assertCount(1, $steps);
        static::assertInstanceOf(BranchPipeline::class, $steps[0]);
        static::assertSame(1, $plan->root()->id);

        foreach ((new Executor())->executePipeline($plan->root(), NodeMother::context()) as $_) {
        }

        static::assertSame([['id' => 1]], $memory->dump());
    }

    public function test_a_node_two_writes_share_off_the_spine_runs_once_in_one_side_pipeline(): void
    {
        $first = new ArrayMemory();
        $second = new ArrayMemory();
        $spy = new SpyTransformer();
        $read = NodeMother::read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $shared = new Node\Transform($read, $spy);

        $plan = config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(
                new Outputs(
                    new Result($read),
                    new Write($shared, to_memory($first)),
                    new Write($shared, to_memory($second)),
                ),
            ));

        static::assertSame([BranchPipeline::class], PipelineStepList::classes($plan->root()));

        foreach ((new Executor())->executePipeline($plan->root(), NodeMother::context()) as $_) {
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

        $plan = config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(
                new Outputs(
                    new Result($read),
                    new Node\Transaction(
                        $transaction,
                        new Write($shared, to_memory(new ArrayMemory())),
                        new Write($shared, to_memory(new ArrayMemory())),
                    ),
                ),
            ));

        static::assertSame([BranchPipeline::class], PipelineStepList::classes($plan->root()));

        foreach ((new Executor())->executePipeline($plan->root(), NodeMother::context()) as $_) {
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

        $plan = config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(
                new Outputs(
                    new Result($read),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($transform, to_memory($shared)),
                        new Write($transform, to_memory(new ArrayMemory())),
                        new Write($read, to_memory($bare)),
                    ),
                ),
            ));

        static::assertSame([TransactionGroup::class], PipelineStepList::classes($plan->root()));

        foreach ((new Executor())->executePipeline($plan->root(), NodeMother::context()) as $_) {
        }

        static::assertSame(2, $spy->seen);
        static::assertSame([['id' => 1], ['id' => 2]], $shared->dump());
        static::assertSame([['id' => 1], ['id' => 2]], $bare->dump());
    }

    public function test_a_write_outside_a_transaction_sharing_a_node_with_one_of_its_children_is_refused(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $shared = NodeMother::select($read);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A write outside a transaction cannot share a node with one of its children');

        config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(
                new Outputs(
                    new Result($read),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($shared, to_memory(new ArrayMemory())),
                        new Write($read, to_memory(new ArrayMemory())),
                    ),
                    new Write($shared, to_memory(new ArrayMemory())),
                ),
            ));
    }

    public function test_a_transaction_is_one_step_over_its_children(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));

        $plan = config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(
                new Outputs(
                    new Result($read),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($read, to_memory(new ArrayMemory())),
                        new Write(NodeMother::select($read), to_memory(new ArrayMemory())),
                    ),
                ),
            ));

        static::assertSame([TransactionGroup::class], PipelineStepList::classes($plan->root()));
        static::assertSame(1, $plan->root()->id);
    }

    public function test_two_writes_at_one_node_are_two_steps(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $first = to_memory(new ArrayMemory());
        $second = to_memory(new ArrayMemory());

        static::assertSame(
            [$first, $second],
            PipelineStepList::of(
                config_builder()
                    ->optimizer(new Optimizer())
                    ->build()
                    ->planner()
                    ->plan(new LogicalPlan(
                        new Outputs(new Result($read), new Write($read, $first), new Write($read, $second)),
                    ))
                    ->root(),
            ),
        );
    }

    public function test_a_write_sharing_no_node_with_the_spine_is_refused(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A write root shares no node with the plan: ' . MemoryLoader::class);

        config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(
                new Outputs(
                    new Result(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
                    new Write(
                        NodeMother::select(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
                        to_memory(new ArrayMemory()),
                    ),
                ),
            ));
    }

    public function test_the_children_of_one_transaction_must_attach_to_the_same_node(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $select = NodeMother::select($read);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('Every write of one transaction must attach to the same node');

        config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(
                new Outputs(
                    new Result($select),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($read, to_memory(new ArrayMemory())),
                        new Write($select, to_memory(new ArrayMemory())),
                    ),
                ),
            ));
    }

    public function test_a_plan_with_an_undescribable_op_still_attaches_its_writes(): void
    {
        $select = NodeMother::select(
            new JoinEach(
                NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
                new StaticExtractorFactory(from_array([['id' => 1]], schema(int_schema('id')))),
                join_on(['id' => 'id']),
                JoinType::inner,
            ),
        );

        $plan = config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(
                new Outputs(
                    new Result(new Collect($select)),
                    new Write($select, to_memory(new ArrayMemory())),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write(NodeMother::select($select), to_memory(new ArrayMemory())),
                    ),
                ),
            ));

        static::assertFalse($plan->described());
        static::assertSame(
            [
                JoinEachRowsTransformer::class,
                SelectColumnsTransformer::class,
                MemoryLoader::class,
                TransactionGroup::class,
                CollectingProcessor::class,
            ],
            PipelineStepList::classes($plan->root()),
        );
    }

    public function test_a_write_that_does_not_translate_to_a_loader_is_refused(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $write = new Write($read, to_memory(new ArrayMemory()));
        $logical = new LogicalPlan(new Outputs(new Result($read), $write));
        $planned = new PlannedNodes();
        config()->planner()->node($logical->root, $planned);
        $planned->add($write, new PlannedNode([], [], null));

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A Write must translate to a Loader, null given');

        config()->planner()->pipelines($logical, $planned);
    }

    public function test_a_batched_transaction_write_is_one_step_behind_the_batch_by_processor(): void
    {
        $batchBy = new Node\BatchBy(
            NodeMother::read(from_array([['transaction_id' => 7]], schema(int_schema('transaction_id')))),
            ref('transaction_id'),
            5_000,
        );

        $plan = config_builder()
            ->optimizer(new Optimizer())
            ->build()
            ->planner()
            ->plan(new LogicalPlan(
                new Outputs(
                    new Result($batchBy),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write(NodeMother::select($batchBy, 'transaction_id'), to_memory(new ArrayMemory())),
                        new Write(new Node\Batch($batchBy, new BatchSize(1_000)), to_memory(new ArrayMemory())),
                    ),
                ),
            ));

        static::assertNull($plan->root()->input());
        static::assertSame(2, $plan->root()->id);
        static::assertSame(
            [BatchingByProcessor::class, TransactionGroup::class],
            PipelineStepList::classes($plan->root()),
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
        config()->planner()->node($logical->root, $planned);

        $plan = config()->planner()->pipelines($logical, $planned);

        static::assertSame(
            5,
            type_instance_of(Read::class)->assert($plan->root()->chain()[0]->input)->criteria()->limit,
        );
        static::assertInstanceOf(Pipeline::class, $plan->root()->input);
    }

    public function test_cut_keeps_steps_without_a_breaker_in_one_pipeline(): void
    {
        $limit = new LimitTransformer(1);
        $offset = new OffsetProcessor(1);

        static::assertEquals(
            [new PipelineSteps([$offset, $limit])],
            array_map(
                static fn(Pipeline $pipeline): PipelineSteps => $pipeline->steps,
                Planner::cut(null, [$offset, $limit], 0),
            ),
        );
    }

    public function test_cut_ends_a_pipeline_at_each_breaker_and_leaves_no_empty_one(): void
    {
        $limit = new LimitTransformer(1);
        $collect = new CollectingProcessor();
        $count = new CountingProcessor();

        static::assertEquals(
            [new PipelineSteps([$limit], $collect), new PipelineSteps([], $count)],
            array_map(
                static fn(Pipeline $pipeline): PipelineSteps => $pipeline->steps,
                Planner::cut(null, [$limit, $collect, $count], 0),
            ),
        );
    }

    public function test_cut_opens_a_pipeline_for_the_steps_after_the_last_breaker(): void
    {
        $collect = new CollectingProcessor();
        $limit = new LimitTransformer(1);

        static::assertEquals(
            [new PipelineSteps([], $collect), new PipelineSteps([$limit])],
            array_map(
                static fn(Pipeline $pipeline): PipelineSteps => $pipeline->steps,
                Planner::cut(null, [$collect, $limit], 0),
            ),
        );
    }

    public function test_cut_ends_a_pipeline_at_a_hash_join_and_streams_a_cross_join(): void
    {
        $right = PhysicalPlanMother::reading(from_rows(array_to_rows([['id' => 1]], schema(int_schema('id')))));
        $cross = new JoinStep(new CrossJoinOperator('r_'), $right);
        $hash = new JoinStep(HashJoinMother::resident(join_on(['id' => 'id'], 'h_'), JoinType::inner), $right);
        $limit = new LimitTransformer(1);

        static::assertEquals(
            [new PipelineSteps([$cross], $hash), new PipelineSteps([$limit])],
            array_map(
                static fn(Pipeline $pipeline): PipelineSteps => $pipeline->steps,
                Planner::cut(null, [$cross, $hash, $limit], 0),
            ),
        );
    }

    public function test_cut_numbers_the_pipelines_from_the_first_id_and_links_each_to_the_one_before(): void
    {
        $read = new Read(from_array([['id' => 1]]));
        $collect = new CollectingProcessor();
        $limit = new LimitTransformer(1);

        $pipelines = Planner::cut($read, [$collect, $limit], 4);

        static::assertCount(2, $pipelines);
        static::assertSame([4, 5], [$pipelines[0]->id, $pipelines[1]->id]);
        static::assertSame($read, $pipelines[0]->input);
        static::assertSame($pipelines[0], $pipelines[1]->input());
        static::assertSame($collect, $pipelines[0]->steps->breaker);
        static::assertSame([$limit], $pipelines[1]->steps->steps);
    }

    public function test_cut_of_a_fed_branch_starts_without_input(): void
    {
        static::assertNull(Planner::cut(null, [new LimitTransformer(1)], 0)[0]->input);
    }
}
