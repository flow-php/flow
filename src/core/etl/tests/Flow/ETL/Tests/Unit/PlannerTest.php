<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use ArrayObject;
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Adapter\CSV\CSVLoader;
use Flow\ETL\Exception\DataDependentSchemaException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Executor\Described;
use Flow\ETL\Executor\Raw;
use Flow\ETL\Executor\SinkFeed;
use Flow\ETL\Extractor\DataFrameExtractor;
use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Loader\MemoryLoader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Optimizer;
use Flow\ETL\Optimizer\Rule\CombineSortAndLimit;
use Flow\ETL\Optimizer\Rule\PushLimitIntoSource;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Plan\Node\JoinEach;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Planner;
use Flow\ETL\Planner\PlannedNodes;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\HashJoinProcessor;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Tests\Context\MemoryTelemetryContext;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\RecordingRule;
use Flow\ETL\Tests\Double\SpineCopyingRule;
use Flow\ETL\Tests\Double\StaticDataFrameFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function array_map;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_array;
use function Flow\ETL\DSL\to_memory;

final class PlannerTest extends FlowTestCase
{
    public function test_the_default_rules_keep_every_sink_on_the_spine(): void
    {
        $read = NodeMother::read();
        $three = NodeMother::limit(NodeMother::limit($read, 5), 3);
        $loader = to_memory(new ArrayMemory());

        $plan = (new Planner(Optimizer::default()))->plan(
            new LogicalPlan(
                new Outputs(
                    new Result(NodeMother::select($three)),
                    new Sinks(
                        new Write($three, $loader),
                        new Write(NodeMother::limit($read, 4), to_memory(new ArrayMemory())),
                    ),
                ),
            ),
            NodeMother::context(),
        );

        $steps = $plan->root()->segments()->steps();
        static::assertSame(4, $plan->root()->limit());
        static::assertSame(
            [SinkFeed::class, LimitTransformer::class, MemoryLoader::class, SelectEntriesTransformer::class],
            array_map(static fn($step) => $step::class, $steps),
        );
        static::assertSame($loader, $steps[2]);
    }

    public function test_a_rule_that_rebuilds_the_plan_around_transform_up_keeps_its_sinks(): void
    {
        $select = NodeMother::select(NodeMother::read());

        $plan = (new Planner(new Optimizer(new SpineCopyingRule())))->plan(
            new LogicalPlan(
                new Outputs(
                    new Result($select),
                    new Sinks(
                        new Write($select, to_memory(new ArrayMemory())),
                        new Write(NodeMother::limit($select, 1), to_memory(new ArrayMemory())),
                    ),
                ),
            ),
            NodeMother::context(),
        );

        // both sinks are children of the one root, so a rule copying the root cannot lose them
        static::assertSame(
            [SelectEntriesTransformer::class, MemoryLoader::class, SinkFeed::class],
            array_map(static fn($step) => $step::class, $plan->root()->segments()->steps()),
        );
    }

    public function test_a_linear_plan_is_one_pipeline_with_the_limit_pushed(): void
    {
        $csv = from_csv(__DIR__ . '/../Fixtures/orders.csv');
        $node = new Read($csv);
        $node = new Select($node, ['id', 'total']);
        $node = new Limit($node, 5);
        $node = new Write($node, to_csv(__DIR__ . '/var/out.csv'));

        $logical = new LogicalPlan(new Result($node));
        $plan = (new Planner(Optimizer::default()))->plan($logical, NodeMother::context());

        static::assertSame(0, $plan->root()->id);
        static::assertSame(
            [SelectEntriesTransformer::class, LimitTransformer::class, CSVLoader::class],
            array_map(static fn($step) => $step::class, $plan->root()->segments()->steps()),
        );
        static::assertNull($plan->root()->input());
        static::assertInstanceOf(Described::class, $plan);
        static::assertSame(['id', 'total'], $plan->schema->references()->names());

        static::assertSame($csv, $plan->root()->segments()->extractor());
        static::assertSame(5, $plan->root()->limit());
        static::assertNull($logical->source()->limit());
    }

    public function test_a_sort_splits_the_plan_into_two_pipelines(): void
    {
        $csv = from_csv(__DIR__ . '/../Fixtures/orders.csv');
        $node = new Read($csv);
        $node = new Sort($node, refs(ref('total')), memory_sort());
        $node = new Limit($node, 5);
        $node = new Write($node, to_csv(__DIR__ . '/var/out.csv'));

        $logical = new LogicalPlan(new Result($node));
        $plan = (new Planner(Optimizer::default()->without(CombineSortAndLimit::class)))->plan(
            $logical,
            NodeMother::context(),
        );

        static::assertSame(1, $plan->root()->id);
        static::assertSame(
            [LimitTransformer::class, CSVLoader::class],
            array_map(static fn($step) => $step::class, $plan->root()->segments()->steps()),
        );
        static::assertNull($plan->root()->segments()->extractor());

        $upstream = $plan->root()->input();

        static::assertNotNull($upstream);
        static::assertSame(0, $upstream->id);
        static::assertSame(
            [MemorySortProcessor::class],
            array_map(static fn($step) => $step::class, $upstream->segments()->steps()),
        );
        static::assertInstanceOf(Described::class, $plan);
        static::assertSame(['id', 'total', 'seller_id'], $plan->schema->references()->names());

        static::assertSame($csv, $upstream->segments()->extractor());
        static::assertNull($upstream->limit());
        static::assertNull($plan->root()->limit());
    }

    public function test_a_join_each_plan_refuses_its_schema_and_has_no_frame_edge(): void
    {
        $node = new Read(from_csv(__DIR__ . '/../Fixtures/orders.csv'));
        $node = new JoinEach(
            $node,
            new StaticDataFrameFactory(df()->read(from_array([['id' => 1]]))),
            join_on(['id' => 'id']),
            JoinType::left,
        );

        $logical = new LogicalPlan(new Result($node));
        $plan = (new Planner(Optimizer::default()))->plan($logical, NodeMother::context());

        static::assertSame(0, $plan->root()->id);
        static::assertSame(
            [JoinEachRowsTransformer::class],
            array_map(static fn($step) => $step::class, $plan->root()->segments()->steps()),
        );
        static::assertInstanceOf(Raw::class, $plan);
        static::assertNull($plan->root()->input());
    }

    public function test_rules_run_once_each_in_registration_order(): void
    {
        $log = new ArrayObject();

        (new Planner(new Optimizer(new RecordingRule('first', $log), new RecordingRule('second', $log))))->plan(
            NodeMother::plan(NodeMother::read()),
            NodeMother::context(),
        );

        static::assertSame(['first', 'second'], $log->getArrayCopy());
    }

    public function test_a_planner_with_no_rules_plans_without_a_push(): void
    {
        $csv = from_csv(__DIR__ . '/../Fixtures/orders.csv');
        $node = new Read($csv);
        $node = new Limit($node, 5);

        $logical = new LogicalPlan(new Result($node));
        $plan = (new Planner())->plan($logical, NodeMother::context());

        static::assertSame($csv, $plan->root()->segments()->extractor());
        static::assertNull($plan->root()->limit());
    }

    public function test_a_join_plans_its_right_side_as_a_side_input(): void
    {
        $right = df()->read($sellers = from_csv(__DIR__ . '/../Fixtures/sellers.csv'));
        $sideInput = NodeMother::frameOf($right);
        $node = new Read(from_csv(__DIR__ . '/../Fixtures/orders.csv'));
        $node = new Node\Join($node, $sideInput, join_on(['seller_id' => 'id'], 'r_'), JoinType::inner);
        $node = new Write($node, to_csv(__DIR__ . '/var/out.csv'));

        $logical = new LogicalPlan(new Result($node));
        $plan = (new Planner(Optimizer::default()))->plan($logical, NodeMother::context());

        static::assertSame(1, $plan->root()->id);
        static::assertSame(
            [CSVLoader::class],
            array_map(static fn($step) => $step::class, $plan->root()->segments()->steps()),
        );

        $upstream = $plan->root()->input();

        static::assertNotNull($upstream);
        static::assertSame(0, $upstream->id);
        static::assertSame(
            [HashJoinProcessor::class],
            array_map(static fn($step) => $step::class, $upstream->segments()->steps()),
        );
        static::assertInstanceOf(Described::class, $plan);
        static::assertSame(['id', 'total', 'seller_id', 'r_id', 'r_name'], $plan->schema->references()->names());

        $side = (new Planner())
            ->node($sideInput, NodeMother::context(), new PlannedNodes())
            ->nestedOrFail()
            ->root();

        static::assertSame([], $side->segments()->steps());
        static::assertSame($sellers, $side->segments()->extractor());
    }

    public function test_a_side_input_subtree_is_planned_with_the_childs_config(): void
    {
        $rightConfig = config();
        $right = df($rightConfig)->read(from_csv(__DIR__ . '/../Fixtures/sellers.csv'));
        $sideInput = NodeMother::frameOf($right);
        $node = new Read(from_csv(__DIR__ . '/../Fixtures/orders.csv'));
        $node = new Node\Join($node, $sideInput, join_on(['seller_id' => 'id'], 'r_'), JoinType::inner);
        $context = NodeMother::context(config());

        $plan = (new Planner(Optimizer::default()))->plan(new LogicalPlan(new Result($node)), $context);
        $side = (new Planner())
            ->node($sideInput, $context, new PlannedNodes())
            ->nestedOrFail()
            ->root();

        static::assertSame($context, $plan->root()->context());
        static::assertSame($rightConfig, $side->context()->config);
        static::assertNotSame($context->config, $side->context()->config);
    }

    public function test_a_nested_plan_read_becomes_this_pipelines_input_edge(): void
    {
        $inner = df()->read(from_csv(__DIR__ . '/../Fixtures/orders.csv'))->select('id');
        $node = new Read($extractor = from_data_frame($inner));
        $node = new Limit($node, 5);
        $node = new Write($node, to_csv(__DIR__ . '/var/out.csv'));

        $logical = new LogicalPlan(new Result($node));
        $plan = (new Planner(Optimizer::default()))->plan($logical, NodeMother::context());

        static::assertSame(0, $plan->root()->id);
        static::assertSame(
            [LimitTransformer::class, CSVLoader::class],
            array_map(static fn($step) => $step::class, $plan->root()->segments()->steps()),
        );
        static::assertSame($extractor, $plan->root()->segments()->extractor());

        $input = $plan->root()->input();

        static::assertNotNull($input);
        static::assertSame(0, $input->id);
        static::assertSame(
            [SelectEntriesTransformer::class, LimitTransformer::class],
            array_map(static fn($step) => $step::class, $input->segments()->steps()),
        );
        static::assertInstanceOf(CSVExtractor::class, $input->segments()->extractor());
        static::assertSame(5, $input->limit());
    }

    public function test_a_nested_plan_read_takes_its_schema_from_the_nested_pipeline(): void
    {
        $counting = new CountingExtractor(schema(int_schema('id'), str_schema('name')));
        $inner = df()->read($counting)->select('id');
        $plan = (new Planner(Optimizer::default()))->plan(
            new LogicalPlan(new Result(new Read(from_data_frame($inner)))),
            NodeMother::context(),
        );

        static::assertInstanceOf(Described::class, $plan);
        static::assertEquals(schema(int_schema('id')), $plan->schema);
        static::assertSame(0, $counting->extractCalls);
    }

    public function test_a_nested_plan_that_declares_a_schema_is_not_inlined(): void
    {
        $inner = df()->read(from_array([['id' => 1]]))->select('id');
        $declaring = from_data_frame($inner)->withSchema(schema(str_schema('id')));
        $node = new Read($declaring);
        $node = new Limit($node, 5);

        $logical = new LogicalPlan(new Result($node));
        $plan = (new Planner(Optimizer::default()))->plan($logical, NodeMother::context());

        static::assertSame(0, $plan->root()->id);
        static::assertNull($plan->root()->input());
        static::assertInstanceOf(Described::class, $plan);
        static::assertEquals(schema(str_schema('id')), $plan->schema);

        $source = $plan->root()->segments()->extractor();

        static::assertInstanceOf(DataFrameExtractor::class, $source);
        static::assertSame($declaring->plan(), $source->plan());
    }

    public function test_a_nodes_schema_is_the_fold_of_its_bound_steps(): void
    {
        $planned = new PlannedNodes();
        $read = new Read(new CountingExtractor(schema(int_schema('id'), str_schema('name'))));

        static::assertEquals(
            schema(int_schema('id'), str_schema('name')),
            (new Planner())->node($read, NodeMother::context(), $planned)->schema,
        );
        static::assertEquals(
            schema(str_schema('name')),
            (new Planner())->node(
                NodeMother::select(NodeMother::select($read, 'id', 'name'), 'name'),
                NodeMother::context(),
                $planned,
            )->schema,
        );
    }

    public function test_a_loader_passes_through_the_fold_unbound(): void
    {
        $output = [];
        $loader = to_array($output);
        $planned = new PlannedNodes();

        $plannedNode = (new Planner())->node(
            new Write(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))), $loader),
            NodeMother::context(),
            $planned,
        );

        static::assertSame([$loader], $plannedNode->steps);
        static::assertSame([$loader], $plannedNode->bound);
        static::assertEquals(schema(int_schema('id')), $plannedNode->schema);
    }

    public function test_bound_steps_are_the_result_of_bind_not_the_translated_instances(): void
    {
        $planned = new PlannedNodes();

        $plannedNode = (new Planner())->node(
            new Node\Collect(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))))),
            NodeMother::context(),
            $planned,
        );

        static::assertInstanceOf(CollectingProcessor::class, $plannedNode->steps[0]);
        static::assertInstanceOf(CollectingProcessor::class, $plannedNode->bound[0]);
        static::assertNotSame($plannedNode->steps[0], $plannedNode->bound[0]);
    }

    public function test_the_same_node_object_is_planned_once(): void
    {
        $planned = new PlannedNodes();
        $node = NodeMother::select(NodeMother::read());

        static::assertSame(
            (new Planner())->node($node, NodeMother::context(), $planned),
            (new Planner())->node($node, NodeMother::context(), $planned),
        );
    }

    public function test_a_frame_is_planned_with_the_childs_context(): void
    {
        $planned = new PlannedNodes();
        $childContext = NodeMother::context(config());
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::read()), $childContext);

        $nested = (new Planner())
            ->node($frame, NodeMother::context(config()), $planned)
            ->nestedOrFail();

        static::assertSame($childContext->config, $nested->root()->context()->config);
    }

    public function test_a_frame_is_optimized_with_its_own_rules(): void
    {
        $childContext = NodeMother::context(
            config_builder()->optimizer(Optimizer::default()->without(PushLimitIntoSource::class))->build(),
        );
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::limit(NodeMother::read(), 5)), $childContext);

        $nested = (new Planner(Optimizer::default()))
            ->node($frame, NodeMother::context(), new PlannedNodes())
            ->nestedOrFail();

        static::assertNull($nested->root()->limit());
    }

    public function test_an_inlined_frame_is_optimized_with_its_own_rules(): void
    {
        $child = df(config_builder()->optimizer(Optimizer::default()->without(PushLimitIntoSource::class)))
            ->read(from_array([['id' => 1]], schema(int_schema('id'))))
            ->limit(5);
        $logical = new LogicalPlan(new Result(new Read(from_data_frame($child))));

        $input = (new Planner(Optimizer::default()))
            ->plan($logical, NodeMother::context())
            ->root()
            ->input();

        static::assertNotNull($input);
        static::assertNull($input->limit());
    }

    public function test_a_limit_pushed_into_an_inlined_frame_is_pushed_into_that_frames_source(): void
    {
        $child = df()->read(from_array([['id' => 1], ['id' => 2]], schema(int_schema('id'))))->select('id');
        $logical = new LogicalPlan(new Result(new Limit(new Read(from_data_frame($child)), 1)));

        $input = (new Planner(Optimizer::default()))
            ->plan($logical, NodeMother::context())
            ->root()
            ->input();

        static::assertNotNull($input);
        static::assertSame(1, $input->limit());
    }

    public function test_a_frame_carries_its_own_plan_as_the_nested_plan(): void
    {
        $planned = new PlannedNodes();
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::select(NodeMother::read(from_array([[
            'id' => 1,
        ]], schema(int_schema('id')))))));

        $nested = (new Planner())
            ->node($frame, NodeMother::context(), $planned)
            ->nestedOrFail();

        static::assertSame(
            [SelectEntriesTransformer::class],
            array_map(static fn($step) => $step::class, $nested->root()->segments()->steps()),
        );
        static::assertInstanceOf(Described::class, $nested);
        static::assertEquals(schema(int_schema('id')), $nested->schema);
        static::assertEquals(schema(int_schema('id')), $planned->of($frame)->schema);
    }

    public function test_a_refusal_anywhere_makes_the_whole_plan_raw(): void
    {
        $planned = new PlannedNodes();
        $joinEach = new JoinEach(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            new StaticDataFrameFactory(df()->read(from_array([['id' => 1]], schema(int_schema('id'))))),
            join_on(['id' => 'id']),
            JoinType::inner,
        );
        $above = NodeMother::select($joinEach);

        (new Planner())->node($above, NodeMother::context(), $planned);

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

    public function test_a_frame_whose_sink_refuses_still_describes_its_rows(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $joinEach = new JoinEach(
            $read,
            new StaticDataFrameFactory(df()->read(from_array([['id' => 1]], schema(int_schema('id'))))),
            join_on(['id' => 'id']),
            JoinType::inner,
        );
        $frame = NodeMother::frame(LogicalPlan::of(
            $read,
            new Sinks(new Write($joinEach, to_memory(new ArrayMemory()))),
        ));
        $planned = new PlannedNodes();

        static::assertEquals(
            schema(int_schema('id')),
            (new Planner())->node($frame, NodeMother::context(), $planned)->schema,
        );
        static::assertNotNull($planned->refusal());
    }

    public function test_the_first_refusal_is_kept(): void
    {
        $planned = new PlannedNodes();
        $factory = new StaticDataFrameFactory(df()->read(from_array([['id' => 1]], schema(int_schema('id')))));
        $first = new JoinEach(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            $factory,
            join_on(['id' => 'id']),
            JoinType::inner,
        );

        (new Planner())->node(
            new JoinEach($first, $factory, join_on(['id' => 'id']), JoinType::left),
            NodeMother::context(),
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
        (new Planner())->node(NodeMother::read(), NodeMother::context(), $planned);

        static::assertFalse($planned->has(NodeMother::read()));
    }

    public function test_a_planned_node_with_a_null_schema_is_distinct_from_a_node_never_walked(): void
    {
        $planned = new PlannedNodes();
        $refusing = new JoinEach(
            NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))),
            new StaticDataFrameFactory(df()->read(from_array([['id' => 1]], schema(int_schema('id'))))),
            join_on(['id' => 'id']),
            JoinType::inner,
        );
        (new Planner())->node($refusing, NodeMother::context(), $planned);

        static::assertTrue($planned->has($refusing));
        static::assertNull($planned->of($refusing)->schema);
        static::assertFalse($planned->has(NodeMother::read()));
    }

    public function test_planning_reads_no_row(): void
    {
        $extractor = new CountingExtractor(schema(int_schema('id')), rows(schema(int_schema('id')), row(['id' => 1])));
        $planned = new PlannedNodes();

        (new Planner())->node(NodeMother::select(new Read($extractor)), NodeMother::context(), $planned);

        static::assertSame(0, $extractor->extractCalls);
    }

    public function test_every_child_is_planned_including_a_side_input(): void
    {
        $planned = new PlannedNodes();
        $read = NodeMother::read();
        $frameRoot = NodeMother::select(NodeMother::read());
        $frame = NodeMother::frame(NodeMother::plan($frameRoot));

        (new Planner())->node(new CrossJoin($read, $frame, 'r_'), NodeMother::context(), $planned);

        static::assertTrue($planned->has($read));
        static::assertTrue($planned->has($frame));
        static::assertTrue($planned->has($frameRoot));
        static::assertTrue($planned->has($frameRoot->children()[0]));
    }

    public function test_a_side_input_is_handed_to_the_translation_as_a_physical_plan(): void
    {
        $planned = new PlannedNodes();
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::read(from_array([[
            'id' => 1,
        ]], schema(int_schema('id'))))));
        $crossJoin = new CrossJoin(NodeMother::read(from_array([['id' => 1]], schema(int_schema('id')))), $frame, 'r_');

        $plannedNode = (new Planner())->node($crossJoin, NodeMother::context(), $planned);

        static::assertInstanceOf(CrossJoinRowsTransformer::class, $plannedNode->steps[0]);
    }

    public function test_a_planning_failure_reports_a_started_and_a_failed_span_and_rethrows(): void
    {
        $telemetry = new MemoryTelemetryContext();

        try {
            (new Planner(Optimizer::default()))->plan(
                NodeMother::plan(NodeMother::select(NodeMother::read(), 'nope')),
                $telemetry->flowContext,
            );

            static::fail('Expected the bind failure to be rethrown.');
        } catch (SchemaDefinitionNotFoundException $e) {
            static::assertSame('Schema definition for entry "nope" not found.', $e->getMessage());
        }

        static::assertCount(1, $telemetry->spans->startedSpans());
        static::assertCount(1, $telemetry->spans->endedSpans());
        static::assertTrue($telemetry->spans->endedSpans()[0]->status()?->isError());
    }
}
