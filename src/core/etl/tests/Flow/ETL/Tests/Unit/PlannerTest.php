<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use ArrayObject;
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Adapter\CSV\CSVLoader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\DataFrameExtractor;
use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Loader\MemoryLoader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Pipeline\SinkFeed;
use Flow\ETL\Plan\Described;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\JoinEach;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Plan\Node\SinkMultiple;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Raw;
use Flow\ETL\Planner;
use Flow\ETL\Planner\Lowerings;
use Flow\ETL\Planner\Rule\CombineLimits;
use Flow\ETL\Planner\Rule\PushFilterIntoSource;
use Flow\ETL\Planner\Rule\PushLimitIntoSource;
use Flow\ETL\Processor\HashJoinProcessor;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\RecordingRule;
use Flow\ETL\Tests\Double\SpineCopyingRule;
use Flow\ETL\Tests\Double\StaticDataFrameFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function array_map;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_memory;

final class PlannerTest extends FlowTestCase
{
    public function test_the_default_rules_keep_every_sink_on_the_spine(): void
    {
        $read = NodeMother::scannableRead();
        $three = NodeMother::limit(NodeMother::limit($read, 5), 3);
        $loader = to_memory(new ArrayMemory());

        $plan = Planner::default()->plan(
            new LogicalPlan(
                new SinkMultiple(
                    new Result(NodeMother::select($three)),
                    new Write($three, $loader),
                    new Write(NodeMother::limit($read, 4), to_memory(new ArrayMemory())),
                ),
            ),
            NodeMother::context(),
        );

        $steps = $plan->root()->segments()->steps();
        static::assertSame(4, $plan->root()->scan()->limit);
        static::assertSame(
            [SinkFeed::class, LimitTransformer::class, MemoryLoader::class, SelectEntriesTransformer::class],
            array_map(static fn($step) => $step::class, $steps),
        );
        static::assertSame($loader, $steps[2]);
    }

    public function test_a_rule_that_rebuilds_the_plan_around_transform_up_keeps_its_sinks(): void
    {
        $select = NodeMother::select(NodeMother::read());

        $plan = (new Planner(Lowerings::default(), new SpineCopyingRule()))->plan(
            new LogicalPlan(
                new SinkMultiple(
                    new Result($select),
                    new Write($select, to_memory(new ArrayMemory())),
                    new Write(NodeMother::limit($select, 1), to_memory(new ArrayMemory())),
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
        $plan = Planner::default()->plan($logical, NodeMother::context());

        static::assertSame(0, $plan->root()->id);
        static::assertSame(
            [SelectEntriesTransformer::class, LimitTransformer::class, CSVLoader::class],
            array_map(static fn($step) => $step::class, $plan->root()->segments()->steps()),
        );
        static::assertNull($plan->root()->input());
        static::assertSame([], $plan->root()->frames());
        static::assertSame([], $plan->root()->dependencies());
        static::assertInstanceOf(Described::class, $plan);
        static::assertSame(['id', 'total'], $plan->schema->references()->names());

        static::assertSame($csv, $plan->root()->segments()->extractor());
        static::assertSame(5, $plan->root()->scan()->limit);
        static::assertNull($logical->source()->scan()->limit);
    }

    public function test_a_sort_splits_the_plan_into_two_pipelines(): void
    {
        $csv = from_csv(__DIR__ . '/../Fixtures/orders.csv');
        $node = new Read($csv);
        $node = new Sort($node, refs(ref('total')), memory_sort());
        $node = new Limit($node, 5);
        $node = new Write($node, to_csv(__DIR__ . '/var/out.csv'));

        $logical = new LogicalPlan(new Result($node));
        $plan = Planner::default()->plan($logical, NodeMother::context());

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
        static::assertSame([$upstream], $plan->root()->dependencies());
        static::assertInstanceOf(Described::class, $plan);
        static::assertSame(['id', 'total', 'seller_id'], $plan->schema->references()->names());

        static::assertSame($csv, $upstream->segments()->extractor());
        static::assertNull($upstream->scan()->limit);
        static::assertNull($plan->root()->scan()->limit);
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
        $plan = Planner::default()->plan($logical, NodeMother::context());

        static::assertSame(0, $plan->root()->id);
        static::assertSame(
            [JoinEachRowsTransformer::class],
            array_map(static fn($step) => $step::class, $plan->root()->segments()->steps()),
        );
        static::assertInstanceOf(Raw::class, $plan);
        static::assertSame([], $plan->root()->frames());
        static::assertSame([], $plan->root()->dependencies());
    }

    public function test_rules_run_once_each_in_registration_order(): void
    {
        $log = new ArrayObject();

        (new Planner(Lowerings::default(), new RecordingRule('first', $log), new RecordingRule('second', $log)))->plan(
            NodeMother::plan(NodeMother::read()),
            NodeMother::context(),
        );

        static::assertSame(['first', 'second'], $log->getArrayCopy());
    }

    public function test_default_registers_combine_limits_push_limit_then_push_filter_into_source(): void
    {
        static::assertSame(
            [CombineLimits::class, PushLimitIntoSource::class, PushFilterIntoSource::class],
            array_map(static fn($rule) => $rule::class, Planner::default()->rules()),
        );
    }

    public function test_without_drops_the_named_rule_and_keeps_the_rest(): void
    {
        static::assertSame(
            [CombineLimits::class, PushFilterIntoSource::class],
            array_map(
                static fn($rule) => $rule::class,
                Planner::default()->without(PushLimitIntoSource::class)->rules(),
            ),
        );
    }

    public function test_without_returns_a_new_planner(): void
    {
        $planner = Planner::default();

        static::assertNotSame($planner, $planner->without(CombineLimits::class));
    }

    public function test_without_throws_on_an_unregistered_rule_name(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(RecordingRule::class . ' is not a registered planner rule');

        Planner::default()->without(RecordingRule::class);
    }

    public function test_a_planner_with_no_rules_plans_without_a_push(): void
    {
        $csv = from_csv(__DIR__ . '/../Fixtures/orders.csv');
        $node = new Read($csv);
        $node = new Limit($node, 5);

        $logical = new LogicalPlan(new Result($node));
        $plan = (new Planner(Lowerings::default()))->plan($logical, NodeMother::context());

        static::assertSame($csv, $plan->root()->segments()->extractor());
        static::assertNull($plan->root()->scan()->limit);
    }

    public function test_a_join_plans_its_right_side_once_as_a_frame_edge(): void
    {
        $right = df()->read($sellers = from_csv(__DIR__ . '/../Fixtures/sellers.csv'));
        $node = new Read(from_csv(__DIR__ . '/../Fixtures/orders.csv'));
        $node = new Node\Join(
            $node,
            NodeMother::frameOf($right),
            join_on(['seller_id' => 'id'], 'r_'),
            JoinType::inner,
        );
        $node = new Write($node, to_csv(__DIR__ . '/var/out.csv'));

        $logical = new LogicalPlan(new Result($node));
        $plan = Planner::default()->plan($logical, NodeMother::context());

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
        static::assertCount(1, $upstream->frames());
        static::assertSame($upstream->frames(), $upstream->dependencies());
        static::assertSame([], $upstream->frames()[0]->segments()->steps());
        static::assertInstanceOf(Described::class, $plan);
        static::assertSame(['id', 'total', 'seller_id', 'r_id', 'r_name'], $plan->schema->references()->names());

        static::assertSame($sellers, $upstream->frames()[0]->segments()->extractor());
    }

    public function test_a_frame_subtree_is_lowered_with_the_childs_config(): void
    {
        $rightConfig = config();
        $right = df($rightConfig)->read(from_csv(__DIR__ . '/../Fixtures/sellers.csv'));
        $node = new Read(from_csv(__DIR__ . '/../Fixtures/orders.csv'));
        $node = new Node\Join(
            $node,
            NodeMother::frameOf($right),
            join_on(['seller_id' => 'id'], 'r_'),
            JoinType::inner,
        );
        $context = NodeMother::context(config());

        $logical = new LogicalPlan(new Result($node));
        $plan = Planner::default()->plan($logical, $context);

        static::assertSame($context, $plan->root()->context());
        static::assertSame($rightConfig, $plan->root()->frames()[0]->context()->config);
        static::assertNotSame($context->config, $plan->root()->frames()[0]->context()->config);
    }

    public function test_a_nested_plan_read_becomes_this_pipelines_input_edge(): void
    {
        $inner = df()->read(from_csv(__DIR__ . '/../Fixtures/orders.csv'))->select('id');
        $node = new Read(from_data_frame($inner));
        $node = new Limit($node, 5);
        $node = new Write($node, to_csv(__DIR__ . '/var/out.csv'));

        $logical = new LogicalPlan(new Result($node));
        $plan = Planner::default()->plan($logical, NodeMother::context());

        static::assertSame(0, $plan->root()->id);
        static::assertSame(
            [LimitTransformer::class, CSVLoader::class],
            array_map(static fn($step) => $step::class, $plan->root()->segments()->steps()),
        );
        static::assertNull($plan->root()->segments()->extractor());

        $input = $plan->root()->input();

        static::assertNotNull($input);
        static::assertSame(0, $input->id);
        static::assertSame(
            [SelectEntriesTransformer::class],
            array_map(static fn($step) => $step::class, $input->segments()->steps()),
        );
        static::assertInstanceOf(CSVExtractor::class, $input->segments()->extractor());
        static::assertSame([$input], $plan->root()->dependencies());
    }

    public function test_a_nested_plan_read_takes_its_schema_from_the_nested_pipeline(): void
    {
        $counting = new CountingExtractor(schema(int_schema('id'), str_schema('name')));
        $inner = df()->read($counting)->select('id');
        $plan = Planner::default()->plan(
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
        $plan = Planner::default()->plan($logical, NodeMother::context());

        static::assertSame(0, $plan->root()->id);
        static::assertNull($plan->root()->input());
        static::assertInstanceOf(Described::class, $plan);
        static::assertEquals(schema(str_schema('id')), $plan->schema);

        $source = $plan->root()->segments()->extractor();

        static::assertInstanceOf(DataFrameExtractor::class, $source);
        static::assertSame($declaring->snapshot(), $source->snapshot());
        static::assertSame([], $plan->root()->dependencies());
    }
}
