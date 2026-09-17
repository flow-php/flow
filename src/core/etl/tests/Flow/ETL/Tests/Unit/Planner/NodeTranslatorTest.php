<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner;

use Flow\ETL\Constraint\UniqueConstraint;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\GroupBy;
use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Node\Aggregate;
use Flow\ETL\Plan\Node\Batch;
use Flow\ETL\Plan\Node\BatchBy;
use Flow\ETL\Plan\Node\Cache;
use Flow\ETL\Plan\Node\Collect;
use Flow\ETL\Plan\Node\CollectRefs;
use Flow\ETL\Plan\Node\Constrain;
use Flow\ETL\Plan\Node\CrossJoin;
use Flow\ETL\Plan\Node\Discard;
use Flow\ETL\Plan\Node\Distinct;
use Flow\ETL\Plan\Node\Drop;
use Flow\ETL\Plan\Node\DuplicateRow;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Join;
use Flow\ETL\Plan\Node\JoinEach;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Offset;
use Flow\ETL\Plan\Node\Rename;
use Flow\ETL\Plan\Node\RenameEach;
use Flow\ETL\Plan\Node\Repartition;
use Flow\ETL\Plan\Node\Select;
use Flow\ETL\Plan\Node\Sort;
use Flow\ETL\Plan\Node\TopN;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Transform;
use Flow\ETL\Plan\Node\Until;
use Flow\ETL\Plan\Node\Validate;
use Flow\ETL\Plan\Node\WindowColumn;
use Flow\ETL\Plan\Node\WithColumn;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Planner\NodeTranslator;
use Flow\ETL\Processor\BatchingByProcessor;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\CachingProcessor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\ConstrainedProcessor;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Processor\HashJoinProcessor;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Processor\OffsetProcessor;
use Flow\ETL\Processor\RepartitionProcessor;
use Flow\ETL\Processor\TopNProcessor;
use Flow\ETL\Processor\VoidProcessor;
use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Schema\Validator\StrictValidator;
use Flow\ETL\Tests\Double\ChildlessNode;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\StaticDataFrameFactory;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;
use Flow\ETL\Transformer\CollectReferencesTransformer;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;
use Flow\ETL\Transformer\DropDuplicatesTransformer;
use Flow\ETL\Transformer\DropEntriesTransformer;
use Flow\ETL\Transformer\DuplicateRowTransformer;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\PruneEntriesTransformer;
use Flow\ETL\Transformer\Rename\RenameMapEntryStrategy;
use Flow\ETL\Transformer\RenameEachEntryTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;
use Flow\ETL\Transformer\UntilTransformer;
use Flow\ETL\WithEntry;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\hash_join;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\window;

final class NodeTranslatorTest extends FlowTestCase
{
    public function test_aggregate_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [PruneEntriesTransformer::class, BucketingProcessor::class, GroupByAggregationProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(
                    new Aggregate(NodeMother::read(), new GroupBy('id'), hash_group_by()),
                    NodeMother::context(),
                    [],
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
                NodeTranslator::toSteps(new BatchBy(NodeMother::read(), ref('id'), 5), NodeMother::context(), []),
            ),
        );
    }

    public function test_batch_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [BatchingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Batch(NodeMother::read(), 10), NodeMother::context(), []),
            ),
        );
    }

    public function test_cache_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CachingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Cache(NodeMother::read(), 'id', null, null), NodeMother::context(), []),
            ),
        );
    }

    public function test_cache_a_batch_size_prepends_a_batching_processor(): void
    {
        static::assertSame(
            [BatchingProcessor::class, CachingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Cache(NodeMother::read(), 'id', 100, null), NodeMother::context(), []),
            ),
        );
    }

    public function test_collect_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CollectingProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Collect(NodeMother::read()), NodeMother::context(), []),
            ),
        );
    }

    public function test_collect_refs_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [CollectReferencesTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new CollectRefs(NodeMother::read(), refs('id')), NodeMother::context(), []),
            ),
        );
    }

    public function test_constrain_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [ConstrainedProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(
                    new Constrain(NodeMother::read(), [new UniqueConstraint('id')]),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }

    public function test_cross_join_steps_are_the_exact_list_in_order(): void
    {
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::read()));
        $right = PhysicalPlanMother::reading(from_array([['id' => 1]], schema(int_schema('id'))));
        $context = NodeMother::context();

        static::assertEquals(
            [new CrossJoinRowsTransformer($right, $context->config->executor(), 'r_')],
            NodeTranslator::toSteps(new CrossJoin(NodeMother::read(), $frame, 'r_'), $context, [$right]),
        );
    }

    public function test_discard_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [VoidProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Discard(NodeMother::read()), NodeMother::context(), []),
            ),
        );
    }

    public function test_distinct_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [DropDuplicatesTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Distinct(NodeMother::read(), ['id']), NodeMother::context(), []),
            ),
        );
    }

    public function test_drop_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [DropEntriesTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Drop(NodeMother::read(), ['id']), NodeMother::context(), []),
            ),
        );
    }

    public function test_duplicate_row_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [DuplicateRowTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new DuplicateRow(NodeMother::read(), lit(true), [new WithEntry(
                    'copy',
                    lit(1),
                )]), NodeMother::context(), []),
            ),
        );
    }

    public function test_filter_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [ScalarFunctionFilterTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(
                    new Filter(NodeMother::read(), ref('id')->equals(lit(1))),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }

    #[TestWith([JoinType::left])]
    #[TestWith([JoinType::left_anti])]
    #[TestWith([JoinType::right])]
    #[TestWith([JoinType::inner])]
    public function test_join_each_the_named_constructor_follows_the_join_type(JoinType $type): void
    {
        $factory = new StaticDataFrameFactory(df()->read(from_array([['id' => 1]])));
        $on = join_on(['id' => 'id']);

        static::assertEquals(
            [match ($type) {
                JoinType::left => JoinEachRowsTransformer::left($factory, $on),
                JoinType::left_anti => JoinEachRowsTransformer::leftAnti($factory, $on),
                JoinType::right => JoinEachRowsTransformer::right($factory, $on),
                JoinType::inner => JoinEachRowsTransformer::inner($factory, $on),
            }],
            NodeTranslator::toSteps(new JoinEach(NodeMother::read(), $factory, $on, $type), NodeMother::context(), []),
        );
    }

    public function test_join_steps_are_the_exact_list_in_order(): void
    {
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::read()));

        static::assertSame(
            [HashJoinProcessor::class],
            array_map(static fn($step) => $step::class, NodeTranslator::toSteps(
                new Join(NodeMother::read(), $frame, join_on(['id' => 'id']), JoinType::inner, hash_join()),
                NodeMother::context(),
                [PhysicalPlanMother::reading(from_array([['id' => 1]]))],
            )),
        );
    }

    public function test_limit_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [LimitTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Limit(NodeMother::read(), 5), NodeMother::context(), []),
            ),
        );
    }

    public function test_offset_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [OffsetProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Offset(NodeMother::read(), 3), NodeMother::context(), []),
            ),
        );
    }

    public function test_read_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame([], NodeTranslator::toSteps(NodeMother::read(), NodeMother::context(), []));
    }

    public function test_rename_each_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [RenameEachEntryTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(
                    new RenameEach(NodeMother::read(), [new RenameMapEntryStrategy(['id' => 'user_id'])]),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }

    public function test_rename_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [RenameEntryTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Rename(NodeMother::read(), 'id', 'user_id'), NodeMother::context(), []),
            ),
        );
    }

    public function test_repartition_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [BucketingProcessor::class, RepartitionProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Repartition(NodeMother::read(), refs('id')), NodeMother::context(), []),
            ),
        );
    }

    public function test_select_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [SelectEntriesTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new Select(NodeMother::read(), ['id']), NodeMother::context(), []),
            ),
        );
    }

    public function test_sort_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [MemorySortProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(
                    new Sort(NodeMother::read(), refs(ref('id')), memory_sort()),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }

    public function test_transaction_a_transaction_owns_no_step(): void
    {
        static::assertSame(
            [],
            NodeTranslator::toSteps(
                new Transaction(
                    new RecordingTransaction(),
                    new Write(NodeMother::read(), to_memory(new ArrayMemory())),
                ),
                NodeMother::context(),
                [],
            ),
        );
    }

    public function test_transform_the_instance_the_node_holds_is_the_step(): void
    {
        $transformer = new SelectEntriesTransformer('id');

        static::assertSame(
            [$transformer],
            NodeTranslator::toSteps(new Transform(NodeMother::read(), $transformer), NodeMother::context(), []),
        );
    }

    public function test_transform_a_stateful_transformer_runs_as_its_fresh_instance(): void
    {
        $transformer = new AddRowIndexTransformer('idx', StartFrom::ZERO);
        $transformer->transform(rows(schema(int_schema('id')), row(['id' => 1])), flow_context());

        $steps = NodeTranslator::toSteps(new Transform(NodeMother::read(), $transformer), NodeMother::context(), []);

        static::assertCount(1, $steps);
        static::assertInstanceOf(AddRowIndexTransformer::class, $steps[0]);
        static::assertNotSame($transformer, $steps[0]);
        static::assertSame(
            [['id' => 1, 'idx' => 0]],
            $steps[0]->transform(rows(schema(int_schema('id')), row(['id' => 1])), flow_context())->toArray(),
        );
    }

    public function test_until_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [UntilTransformer::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(
                    new Until(NodeMother::read(), ref('id')->equals(lit(1))),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }

    public function test_validate_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [SchemaValidationLoader::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(
                    new Validate(NodeMother::read(), schema(int_schema('id')), new StrictValidator()),
                    NodeMother::context(),
                    [],
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
                NodeTranslator::toSteps(
                    new WindowColumn(NodeMother::read(), 'rank', rank()->over(window()->orderBy(ref('id')))),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }

    public function test_window_column_a_partitioned_window_translates_to_repartition_steps_then_the_window_processor(): void
    {
        static::assertSame(
            [BucketingProcessor::class, RepartitionProcessor::class, WindowProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(
                    new WindowColumn(
                        NodeMother::read(),
                        'rank',
                        rank()->over(window()->partitionBy(ref('group'))->orderBy(ref('id'))),
                    ),
                    NodeMother::context(),
                    [],
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
                NodeTranslator::toSteps(
                    new WithColumn(NodeMother::read(), 'doubled', ref('id')->multiply(lit(2))),
                    NodeMother::context(),
                    [],
                ),
            ),
        );
    }

    public function test_write_the_instance_the_node_holds_is_the_step(): void
    {
        $loader = to_memory(new ArrayMemory());

        static::assertSame(
            [$loader],
            NodeTranslator::toSteps(new Write(NodeMother::read(), $loader), NodeMother::context(), []),
        );
    }

    public function test_a_node_without_a_translation_throws(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('No physical steps are known for node ' . ChildlessNode::class);

        NodeTranslator::toSteps(new ChildlessNode(), NodeMother::context(), []);
    }

    public function test_top_n_steps_are_the_exact_list_in_order(): void
    {
        static::assertSame(
            [TopNProcessor::class],
            array_map(
                static fn($step) => $step::class,
                NodeTranslator::toSteps(new TopN(NodeMother::read(), refs(ref('id')), 3), NodeMother::context(), []),
            ),
        );
    }
}
