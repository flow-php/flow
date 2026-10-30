<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Physical;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\IO\Loader\SchemaValidationLoader;
use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Operators\Constraint\UniqueConstraint;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\Pivot;
use Flow\ETL\Operators\Join\CrossJoin as CrossJoinOperator;
use Flow\ETL\Operators\Join\HashJoin;
use Flow\ETL\Operators\Processor\BatchingByProcessor;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Operators\Processor\CachingProcessor;
use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Operators\Processor\ConstrainedProcessor;
use Flow\ETL\Operators\Processor\CountingProcessor;
use Flow\ETL\Operators\Processor\DistinctProcessor;
use Flow\ETL\Operators\Processor\ExpandingProcessor;
use Flow\ETL\Operators\Processor\GroupByAggregationProcessor;
use Flow\ETL\Operators\Processor\MemorySortProcessor;
use Flow\ETL\Operators\Processor\MergeSortProcessor;
use Flow\ETL\Operators\Processor\OffsetProcessor;
use Flow\ETL\Operators\Processor\PivotProcessor;
use Flow\ETL\Operators\Processor\RepartitionProcessor;
use Flow\ETL\Operators\Processor\TopNProcessor;
use Flow\ETL\Operators\Processor\VoidProcessor;
use Flow\ETL\Operators\Processor\WindowProcessor;
use Flow\ETL\Operators\Transformer\AddRowIndex\StartFrom;
use Flow\ETL\Operators\Transformer\AddRowIndexTransformer;
use Flow\ETL\Operators\Transformer\CollectReferencesTransformer;
use Flow\ETL\Operators\Transformer\DropColumnsTransformer;
use Flow\ETL\Operators\Transformer\DuplicateRowTransformer;
use Flow\ETL\Operators\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Operators\Transformer\PruneColumnsTransformer;
use Flow\ETL\Operators\Transformer\Rename\RenameMapColumnStrategy;
use Flow\ETL\Operators\Transformer\RenameColumnTransformer;
use Flow\ETL\Operators\Transformer\RenameEachColumnTransformer;
use Flow\ETL\Operators\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\Operators\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Operators\Transformer\SelectColumnsTransformer;
use Flow\ETL\Operators\Transformer\UntilTransformer;
use Flow\ETL\Planning\Logical\Node\Aggregate;
use Flow\ETL\Planning\Logical\Node\Batch;
use Flow\ETL\Planning\Logical\Node\BatchBy;
use Flow\ETL\Planning\Logical\Node\Cache;
use Flow\ETL\Planning\Logical\Node\Collect;
use Flow\ETL\Planning\Logical\Node\CollectRefs;
use Flow\ETL\Planning\Logical\Node\Constrain;
use Flow\ETL\Planning\Logical\Node\Count;
use Flow\ETL\Planning\Logical\Node\CrossJoin;
use Flow\ETL\Planning\Logical\Node\Discard;
use Flow\ETL\Planning\Logical\Node\Distinct;
use Flow\ETL\Planning\Logical\Node\Drop;
use Flow\ETL\Planning\Logical\Node\DuplicateRow;
use Flow\ETL\Planning\Logical\Node\ExpandColumn;
use Flow\ETL\Planning\Logical\Node\Filter;
use Flow\ETL\Planning\Logical\Node\Join;
use Flow\ETL\Planning\Logical\Node\JoinEach;
use Flow\ETL\Planning\Logical\Node\Limit;
use Flow\ETL\Planning\Logical\Node\Offset;
use Flow\ETL\Planning\Logical\Node\Rename;
use Flow\ETL\Planning\Logical\Node\RenameEach;
use Flow\ETL\Planning\Logical\Node\Repartition;
use Flow\ETL\Planning\Logical\Node\Select;
use Flow\ETL\Planning\Logical\Node\Sort;
use Flow\ETL\Planning\Logical\Node\TopN;
use Flow\ETL\Planning\Logical\Node\Transaction;
use Flow\ETL\Planning\Logical\Node\Transform;
use Flow\ETL\Planning\Logical\Node\Until;
use Flow\ETL\Planning\Logical\Node\Validate;
use Flow\ETL\Planning\Logical\Node\WindowColumn;
use Flow\ETL\Planning\Logical\Node\WithColumn;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Runtime\Config;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\Double\ChildlessNode;
use Flow\ETL\Tests\Double\FixedRandomValueGenerator;
use Flow\ETL\Tests\Double\IdRecordingRowsStorage;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\SpyRowsStorage;
use Flow\ETL\Tests\Double\StaticExtractorFactory;
use Flow\ETL\Tests\Double\UnenumerableSum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\NodeTranslatorMother;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Schema\Validator\StrictValidator;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\partition_by;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\window;
use function Flow\ETL\DSL\with_column;
use function Flow\Filesystem\DSL\path;
use function Flow\Floe\DSL\to_floe;

final class NodeTranslatorTest extends FlowTestCase
{
    public function test_aggregate_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [PruneColumnsTransformer::class, GroupByAggregationProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(
                    new Aggregate(NodeMother::read(), new GroupBy(refs('id')), hash_group_by()),
                ),
            ),
        );
    }

    public function test_batch_by_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [BatchingByProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new BatchBy(NodeMother::read(), ref('id'), 5)),
            ),
        );
    }

    public function test_batch_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [BatchingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Batch(NodeMother::read(), new BatchSize(10))),
            ),
        );
    }

    public function test_cache_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CachingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Cache(NodeMother::read(), 'id', null, null)),
            ),
        );
    }

    public function test_cache_without_an_id_or_storage_takes_the_run_config_ones(): void
    {
        $storage = new MemoryRowsStorage();
        $steps = NodeTranslatorMother::of(config_builder()->cache($storage)->id('my-pipeline-id')->build())->steps(
            new Cache(NodeMother::read(), null, null, null),
        );

        static::assertInstanceOf(CachingProcessor::class, $steps[0]);
        static::assertSame('my-pipeline-id', $steps[0]->id);
        static::assertSame($storage, $steps[0]->cache);
        static::assertFalse($steps[0]->idGiven);
    }

    public function test_cache_a_batch_size_prepends_a_batching_processor(): void
    {
        static::assertSame(
            [BatchingProcessor::class, CachingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Cache(NodeMother::read(), 'id', 100, null)),
            ),
        );
    }

    public function test_collect_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CollectingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Collect(NodeMother::read())),
            ),
        );
    }

    public function test_collect_refs_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CollectReferencesTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new CollectRefs(NodeMother::read(), refs('id'))),
            ),
        );
    }

    public function test_constrain_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [ConstrainedProcessor::class],
            array_map(static fn($step) => $step::class, NodeTranslatorMother::of(config())->steps(
                new Constrain(NodeMother::read(), [new UniqueConstraint('id')]),
            )),
        );
    }

    public function test_constrain_a_stateful_constraint_runs_as_its_fresh_instance(): void
    {
        $constraint = new UniqueConstraint('id');
        $constraint->firstViolation(array_to_rows([['id' => 1]], schema(int_schema('id'))));

        $steps = NodeTranslatorMother::of(config())->steps(new Constrain(NodeMother::read(), [$constraint]));

        static::assertInstanceOf(ConstrainedProcessor::class, $steps[0]);
        static::assertNotSame($constraint, $steps[0]->constraints[0]);
        static::assertNull($steps[0]->constraints[0]->firstViolation(array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id')))));
    }

    public function test_cross_join_steps_are_the_exact_list_in_order(): void
    {
        $frame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));
        $context = NodeMother::context();

        static::assertEquals(
            [new CrossJoinOperator('r_')],
            NodeTranslatorMother::of($context->config)->steps(new CrossJoin(NodeMother::read(), $frame, 'r_')),
        );
    }

    public function test_count_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CountingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Count(NodeMother::read())),
            ),
        );
    }

    public function test_discard_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [VoidProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Discard(NodeMother::read())),
            ),
        );
    }

    public function test_distinct_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [DistinctProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Distinct(NodeMother::read(), ['id'])),
            ),
        );
    }

    public function test_drop_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [DropColumnsTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Drop(NodeMother::read(), ['id'])),
            ),
        );
    }

    public function test_duplicate_row_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [DuplicateRowTransformer::class],
            array_map(static fn($step) => $step::class, NodeTranslatorMother::of(config())->steps(new DuplicateRow(
                NodeMother::read(),
                lit(true),
                [with_column('copy', lit(1))],
            ))),
        );
    }

    public function test_filter_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [ScalarFunctionFilterTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Filter(NodeMother::read(), ref('id')->equals(lit(1)))),
            ),
        );
    }

    #[TestWith([JoinType::left])]
    #[TestWith([JoinType::left_anti])]
    #[TestWith([JoinType::right])]
    #[TestWith([JoinType::inner])]
    public function test_join_each_the_named_constructor_follows_the_join_type(JoinType $type): void
    {
        $factory = new StaticExtractorFactory(from_array([['id' => 1]]));
        $on = join_on(['id' => 'id']);

        static::assertEquals(
            [match ($type) {
                JoinType::left => JoinEachRowsTransformer::left($factory, $on),
                JoinType::left_anti => JoinEachRowsTransformer::leftAnti($factory, $on),
                JoinType::right => JoinEachRowsTransformer::right($factory, $on),
                JoinType::inner => JoinEachRowsTransformer::inner($factory, $on),
            }],
            NodeTranslatorMother::of(config())->steps(new JoinEach(NodeMother::read(), $factory, $on, $type)),
        );
    }

    public function test_join_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [HashJoin::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(
                    new Join(
                        NodeMother::read(),
                        NodeMother::joinRight(NodeMother::plan(NodeMother::read())),
                        join_on(['id' => 'id']),
                        JoinType::inner,
                        hash_join(),
                    ),
                ),
            ),
        );
    }

    public function test_limit_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [LimitTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Limit(NodeMother::read(), 5)),
            ),
        );
    }

    public function test_offset_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [OffsetProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Offset(NodeMother::read(), 3)),
            ),
        );
    }

    public function test_read_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame([], NodeTranslatorMother::of(config())->steps(NodeMother::read()));
    }

    public function test_rename_each_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [RenameEachColumnTransformer::class],
            array_map(static fn($step) => $step::class, NodeTranslatorMother::of(config())->steps(
                new RenameEach(NodeMother::read(), [new RenameMapColumnStrategy(['id' => 'user_id'])]),
            )),
        );
    }

    public function test_rename_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [RenameColumnTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Rename(NodeMother::read(), 'id', 'user_id')),
            ),
        );
    }

    public function test_repartition_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [RepartitionProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Repartition(NodeMother::read(), refs('id'))),
            ),
        );
    }

    public function test_select_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [SelectColumnsTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Select(NodeMother::read(), ['id'])),
            ),
        );
    }

    public function test_sort_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [MemorySortProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(
                    new Sort(NodeMother::read(), [ref('id')->asc()], memory_sort()),
                ),
            ),
        );
    }

    public function test_transaction_a_transaction_owns_no_step(): void
    {
        static::assertSame(
            [],
            NodeTranslatorMother::of(config())->steps(
                new Transaction(
                    new RecordingTransaction(),
                    new Write(NodeMother::read(), to_memory(new ArrayMemory())),
                ),
            ),
        );
    }

    public function test_transform_the_instance_the_node_holds_is_the_step(): void
    {
        $transformer = new SelectColumnsTransformer('id');

        static::assertSame(
            [$transformer],
            NodeTranslatorMother::of(config())->steps(new Transform(NodeMother::read(), $transformer)),
        );
    }

    public function test_transform_a_stateful_transformer_runs_as_its_fresh_instance(): void
    {
        $transformer = new AddRowIndexTransformer('idx', StartFrom::ZERO);
        $transformer->transform(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend());

        $steps = NodeTranslatorMother::of(config())->steps(new Transform(NodeMother::read(), $transformer));

        static::assertCount(1, $steps);
        static::assertInstanceOf(AddRowIndexTransformer::class, $steps[0]);
        static::assertNotSame($transformer, $steps[0]);
        static::assertSame(
            [['id' => 1, 'idx' => 0]],
            $steps[0]
                ->transform(array_to_rows([['id' => 1]], schema(int_schema('id'))), flow_context()->backend())
                ->toArray(),
        );
    }

    public function test_until_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [UntilTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new Until(NodeMother::read(), ref('id')->equals(lit(1)))),
            ),
        );
    }

    public function test_validate_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [SchemaValidationLoader::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(
                    new Validate(NodeMother::read(), schema(int_schema('id')), new StrictValidator()),
                ),
            ),
        );
    }

    public function test_window_column_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CollectingProcessor::class, WindowProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(
                    new WindowColumn(NodeMother::read(), 'rank', rank()->over(window()->orderBy(ref('id')))),
                ),
            ),
        );
    }

    public function test_window_column_a_partitioned_window_translates_to_repartition_steps_then_the_window_processor(): void
    {
        static::assertSame(
            [RepartitionProcessor::class, WindowProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(
                    new WindowColumn(
                        NodeMother::read(),
                        'rank',
                        rank()->over(window()->partitionBy(ref('group'))->orderBy(ref('id'))),
                    ),
                ),
            ),
        );
    }

    public function test_with_column_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [ScalarFunctionTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(
                    new WithColumn(NodeMother::read(), 'doubled', ref('id')->multiply(lit(2))),
                ),
            ),
        );
    }

    public function test_expand_column_runs_on_one_expanding_processor(): void
    {
        static::assertSame(
            [ExpandingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(
                    new ExpandColumn(NodeMother::read(), 'item', ref('items')->expand()),
                ),
            ),
        );
    }

    public function test_write_the_instance_the_node_holds_is_the_step(): void
    {
        $loader = to_memory(new ArrayMemory());

        static::assertSame(
            [$loader],
            NodeTranslatorMother::of(config())->steps(new Write(NodeMother::read(), $loader)),
        );
    }

    public function test_a_node_without_a_translation_throws(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('No physical steps are known for node ' . ChildlessNode::class);

        NodeTranslatorMother::of(config())->steps(new ChildlessNode());
    }

    public function test_top_n_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [TopNProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(new TopN(NodeMother::read(), [ref('id')->asc()], 3)),
            ),
        );
    }

    public function test_a_write_partitioned_by_part_into_a_part_destination_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('cannot name the partition column "part"');

        NodeTranslatorMother::of(config())->steps(
            new Write(
                NodeMother::read(),
                to_floe(path('file:///tmp/out/{part}/rows-{part}.floe'))->partitionBy(partition_by('part')),
            ),
        );
    }

    public function test_aggregate_a_pivot_is_one_pivot_processor(): void
    {
        static::assertSame(
            [PivotProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(
                    new Aggregate(
                        NodeMother::read(),
                        new GroupBy(
                            refs(ref('date')),
                            new Aggregators(sum(ref('contributions'))),
                            new Pivot(ref('user'), pivot_values('norbert')),
                        ),
                    ),
                ),
            ),
        );
    }

    public function test_aggregate_an_aggregation_that_names_no_columns_prunes_nothing(): void
    {
        static::assertSame(
            [GroupByAggregationProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config())->steps(
                    new Aggregate(
                        NodeMother::read(),
                        new GroupBy(refs(ref('category')), new Aggregators(new UnenumerableSum(ref('amount')))),
                    ),
                ),
            ),
        );
    }

    public function test_aggregate_a_pinned_algorithm_replaces_the_frame_config(): void
    {
        $steps = NodeTranslatorMother::of(config_builder()->groupBy(hash_group_by()->bucketsCount(5))->build())->steps(
            new Aggregate(NodeMother::read(), new GroupBy(refs('id')), hash_group_by()->bucketsCount(2)),
        );

        static::assertInstanceOf(GroupByAggregationProcessor::class, $steps[1]);
        static::assertSame(2, $steps[1]->config->bucketsCount);
    }

    public function test_aggregate_the_configs_random_generator_names_the_spill_buckets(): void
    {
        $storage = new IdRecordingRowsStorage();
        $base = config_builder()
            ->groupBy(hash_group_by()->storage($storage)->memoryLimit(Unit::fromBytes(1))->batchSize(1))
            ->build();

        GroupByContext::aggregate(
            new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
            flow_context(
                new Config(
                    $base->id(),
                    $base->name(),
                    $base->version(),
                    $base->serializer(),
                    $base->clock(),
                    $base->optimizer(),
                    $base->executor(),
                    $base->cache,
                    $base->sort,
                    $base->analyze(),
                    $base->telemetry,
                    $base->grouping,
                    $base->join,
                    $base->repartition,
                    $base->backend(),
                    new FixedRandomValueGenerator('fixed'),
                ),
            ),
            array_to_rows([['category' => 'a', 'amount' => 10]], schema(str_schema('category'), int_schema('amount'))),
            array_to_rows([['category' => 'b', 'amount' => 15]], schema(str_schema('category'), int_schema('amount'))),
        );

        static::assertNotSame([], $storage->ids);

        foreach ($storage->ids as $id) {
            static::assertStringStartsWith('group-by-fixed-', $id);
        }
    }

    public function test_aggregate_spills_only_the_grouping_and_aggregation_columns(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());

        GroupByContext::aggregate(
            new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
            flow_context(
                config_builder()
                    ->groupBy(hash_group_by()->storage($storage)->memoryLimit(Unit::fromBytes(1))->batchSize(1))
                    ->build(),
            ),
            array_to_rows(
                [['category' => 'a', 'amount' => 10, 'noise' => 'x', 'id' => 1]],
                schema(str_schema('category'), int_schema('amount'), str_schema('noise'), int_schema('id')),
            ),
            array_to_rows(
                [['category' => 'b', 'amount' => 15, 'noise' => 'y', 'id' => 2]],
                schema(str_schema('category'), int_schema('amount'), str_schema('noise'), int_schema('id')),
            ),
        );

        static::assertNotSame([], $storage->appendedRows());

        foreach ($storage->appendedRows() as $batches) {
            foreach ($batches as $batch) {
                foreach ($batch->toArray() as $spilledRow) {
                    static::assertSame(['category', 'amount'], array_keys($spilledRow));
                }
            }
        }
    }

    public function test_aggregate_spills_every_column_when_an_aggregation_names_no_columns(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());

        $result = GroupByContext::aggregate(
            new GroupBy(refs(ref('category')), new Aggregators(new UnenumerableSum(ref('amount')))),
            flow_context(
                config_builder()
                    ->groupBy(hash_group_by()->storage($storage)->memoryLimit(Unit::fromBytes(1))->batchSize(1))
                    ->build(),
            ),
            array_to_rows(
                [['category' => 'a', 'amount' => 0.5, 'flag' => true]],
                schema(str_schema('category'), float_schema('amount'), bool_schema('flag')),
            ),
            array_to_rows(
                [['category' => 'a', 'amount' => 0.25, 'flag' => true]],
                schema(str_schema('category'), float_schema('amount'), bool_schema('flag')),
            ),
        );

        static::assertNotSame([], $storage->appendedRows());

        foreach ($storage->appendedRows() as $batches) {
            foreach ($batches as $batch) {
                foreach ($batch->toArray() as $spilledRow) {
                    static::assertSame(['category', 'amount', 'flag'], array_keys($spilledRow));
                }
            }
        }

        static::assertSame(0.75, $result[0]->column('amount_sum')->value(0));
    }

    public function test_join_a_pinned_algorithm_replaces_the_frame_config(): void
    {
        $steps = NodeTranslatorMother::of(config_builder()->join(hash_join()->bucketsCount(5))->build())->steps(
            new Join(
                NodeMother::read(),
                NodeMother::joinRight(NodeMother::plan(NodeMother::read())),
                join_on(['id' => 'id']),
                JoinType::inner,
                hash_join()->bucketsCount(3),
            ),
        );

        static::assertInstanceOf(HashJoin::class, $steps[0]);
        static::assertSame(3, $steps[0]->config->bucketing->bucketsCount);
    }

    public function test_repartition_runs_on_the_frame_repartition_config(): void
    {
        $context = NodeMother::context();
        $steps = NodeTranslatorMother::of($context->config)->steps(new Repartition(NodeMother::read(), refs('id')));

        static::assertInstanceOf(RepartitionProcessor::class, $steps[0]);
        static::assertSame($context->config->repartition, $steps[0]->config);
    }

    public function test_sort_external_sort_is_the_frame_default(): void
    {
        static::assertSame(
            [MergeSortProcessor::class],
            array_map(static fn($step) => $step::class, NodeTranslatorMother::of(config())->steps(
                new Sort(NodeMother::read(), [ref('id')->asc()]),
            )),
        );
    }

    public function test_sort_a_pinned_algorithm_replaces_the_frame_config(): void
    {
        static::assertSame(
            [MergeSortProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslatorMother::of(config_builder()->sort(memory_sort())->build())->steps(
                    new Sort(NodeMother::read(), [ref('id')->asc()], external_sort()->storage(new MemoryRowsStorage())),
                ),
            ),
        );
    }

    public function test_top_n_a_pinned_algorithm_replaces_the_frame_config(): void
    {
        $steps = NodeTranslatorMother::of(config())->steps(
            new TopN(NodeMother::read(), [ref('id')->asc()], 3, memory_sort()),
        );

        static::assertInstanceOf(TopNProcessor::class, $steps[0]);
        static::assertInstanceOf(MemorySortConfig::class, $steps[0]->config);
    }
}
