<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner\Rule;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\SinkMultiple;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Planner\Rule\PushLimitIntoSource;
use Flow\ETL\Tests\Double\ChildlessNode;
use Flow\ETL\Tests\Double\RecordingScanExtractor;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\structure;
use function Flow\ETL\DSL\to_memory;

final class PushLimitIntoSourceTest extends FlowTestCase
{
    public function test_a_limit_directly_above_the_read_is_pushed(): void
    {
        $plan = (new PushLimitIntoSource())->apply(
            NodeMother::plan(NodeMother::limit(NodeMother::scannableRead(), 10)),
            NodeMother::context(),
        );

        static::assertSame(10, $plan->source()->scan()->limit);
    }

    public function test_a_limit_passes_a_select(): void
    {
        $plan = (new PushLimitIntoSource())->apply(
            NodeMother::plan(NodeMother::limit(NodeMother::select(NodeMother::scannableRead()), 10)),
            NodeMother::context(),
        );

        static::assertSame(10, $plan->source()->scan()->limit);
    }

    #[DataProvider('expanding_expressions')]
    public function test_a_limit_does_not_pass_a_with_column_that_expands(ScalarFunction $function): void
    {
        $plan = (new PushLimitIntoSource())->apply(
            NodeMother::plan(NodeMother::limit(
                new Node\WithColumn(NodeMother::scannableRead(), 'expanded', $function),
                10,
            )),
            NodeMother::context(),
        );

        static::assertNull($plan->source()->scan()->limit);
    }

    public static function expanding_expressions(): Generator
    {
        yield 'root expand' => [ref('data')->expand()];
        yield 'nested expand' => [structure(['tag' => ref('data')->expand()])];
    }

    public function test_a_limit_does_not_pass_a_filter(): void
    {
        $plan = (new PushLimitIntoSource())->apply(
            NodeMother::plan(NodeMother::limit(
                new Node\Filter(NodeMother::select(NodeMother::scannableRead()), ref('id')->equals(lit(1))),
                10,
            )),
            NodeMother::context(),
        );

        static::assertNull($plan->source()->scan()->limit);
    }

    public function test_a_limit_does_not_pass_a_write(): void
    {
        $plan = (new PushLimitIntoSource())->apply(
            NodeMother::plan(NodeMother::limit(
                new Node\Write(NodeMother::scannableRead(), to_memory(new ArrayMemory())),
                10,
            )),
            NodeMother::context(),
        );

        static::assertNull($plan->source()->scan()->limit);
    }

    public function test_a_blocker_discards_the_limit_collected_above_it_and_the_walk_continues(): void
    {
        $plan = (new PushLimitIntoSource())->apply(
            NodeMother::plan(NodeMother::limit(
                new Node\Sort(NodeMother::limit(NodeMother::scannableRead(), 5), refs(ref('id')), memory_sort()),
                3,
            )),
            NodeMother::context(),
        );

        static::assertSame(5, $plan->source()->scan()->limit);
    }

    public function test_a_non_push_down_extractor_is_left_alone(): void
    {
        $plan = NodeMother::plan(NodeMother::limit(
            NodeMother::select(NodeMother::read(from_array([['id' => 1]]))),
            10,
        ));

        static::assertSame($plan, (new PushLimitIntoSource())->apply($plan, NodeMother::context()));
    }

    public function test_the_push_rewrites_the_leaf_and_keeps_the_spine(): void
    {
        $extractor = new RecordingScanExtractor(schema(int_schema('id')));
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::select(new Read($extractor)), 10));

        $pushed = (new PushLimitIntoSource())->apply($plan, NodeMother::context());

        static::assertNotSame($plan, $pushed);
        $limit = $pushed->root->children()[0];
        static::assertInstanceOf(Node\Limit::class, $limit);
        static::assertSame(10, $limit->limit);
        static::assertInstanceOf(Node\Select::class, $limit->children()[0]);
        static::assertSame(10, $pushed->source()->scan()->limit);
        static::assertSame($extractor, $pushed->source()->extractor());
        static::assertNull($plan->source()->scan()->limit);
        static::assertSame([], $extractor->scans);
    }

    public function test_the_minimum_of_two_limits_is_pushed(): void
    {
        $plan = (new PushLimitIntoSource())->apply(
            NodeMother::plan(NodeMother::limit(
                NodeMother::select(NodeMother::limit(NodeMother::scannableRead(), 10)),
                3,
            )),
            NodeMother::context(),
        );

        static::assertSame(3, $plan->source()->scan()->limit);
    }

    public function test_a_limit_narrows_an_already_pushed_limit(): void
    {
        $plan = (new PushLimitIntoSource())->apply(
            NodeMother::plan(NodeMother::limit(
                new Node\Rename(NodeMother::scannableRead()->withLimit(4), 'id', 'new_id'),
                10,
            )),
            NodeMother::context(),
        );

        static::assertSame(4, $plan->source()->scan()->limit);
    }

    public function test_a_plan_without_a_limit_pushes_nothing(): void
    {
        $plan = (new PushLimitIntoSource())->apply(
            NodeMother::plan(NodeMother::select(NodeMother::scannableRead())),
            NodeMother::context(),
        );

        static::assertNull($plan->source()->scan()->limit);
    }

    public function test_a_row_input_chain_that_does_not_end_in_a_read_throws(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A logical plan must end in a Read, ' . ChildlessNode::class . ' found');

        (new PushLimitIntoSource())->apply(
            NodeMother::plan(NodeMother::limit(NodeMother::select(new ChildlessNode()), 10)),
            NodeMother::context(),
        );
    }

    public function test_a_transaction_contributes_one_walk_per_child(): void
    {
        $limit = NodeMother::limit(NodeMother::scannableRead(), 3);

        $plan = (new PushLimitIntoSource())->apply(
            new LogicalPlan(
                new SinkMultiple(
                    new Result($limit),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($limit, to_memory(new ArrayMemory())),
                        new Write($limit, to_memory(new ArrayMemory())),
                    ),
                ),
            ),
            NodeMother::context(),
        );

        static::assertSame(3, $plan->source()->scan()->limit);
    }

    public function test_one_transaction_child_whose_walk_yields_no_limit_blocks_the_push(): void
    {
        $read = NodeMother::scannableRead();
        $limit = NodeMother::limit($read, 3);

        $plan = (new PushLimitIntoSource())->apply(
            new LogicalPlan(
                new SinkMultiple(
                    new Result($limit),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($limit, to_memory(new ArrayMemory())),
                        new Write($read, to_memory(new ArrayMemory())),
                    ),
                ),
            ),
            NodeMother::context(),
        );

        static::assertNull($plan->source()->scan()->limit);
    }

    public function test_a_limit_below_a_sink_is_pushed(): void
    {
        $limit = NodeMother::limit(NodeMother::scannableRead(), 3);

        $plan = (new PushLimitIntoSource())->apply(
            new LogicalPlan(new SinkMultiple(new Result($limit), new Write($limit, to_memory(new ArrayMemory())))),
            NodeMother::context(),
        );

        static::assertSame(3, $plan->source()->scan()->limit);
    }

    public function test_a_limit_above_a_sink_does_not_narrow_the_sink(): void
    {
        $read = NodeMother::scannableRead();

        $plan = (new PushLimitIntoSource())->apply(
            new LogicalPlan(
                new SinkMultiple(
                    new Result(NodeMother::limit($read, 3)),
                    new Write($read, to_memory(new ArrayMemory())),
                ),
            ),
            NodeMother::context(),
        );

        static::assertNull($plan->source()->scan()->limit);
    }

    public function test_the_widest_limit_of_every_root_is_pushed(): void
    {
        $limit = NodeMother::limit(NodeMother::scannableRead(), 5);

        $plan = (new PushLimitIntoSource())->apply(
            new LogicalPlan(
                new SinkMultiple(
                    new Result(NodeMother::limit($limit, 3)),
                    new Write($limit, to_memory(new ArrayMemory())),
                ),
            ),
            NodeMother::context(),
        );

        static::assertSame(5, $plan->source()->scan()->limit);
    }

    public function test_a_sink_whose_walk_yields_no_limit_blocks_the_push(): void
    {
        $read = NodeMother::scannableRead();

        $plan = (new PushLimitIntoSource())->apply(
            new LogicalPlan(
                new SinkMultiple(
                    new Result(NodeMother::limit($read, 3)),
                    new Write(
                        new Node\Transform($read, new AddRowIndexTransformer('idx', StartFrom::ZERO)),
                        to_memory(new ArrayMemory()),
                    ),
                ),
            ),
            NodeMother::context(),
        );

        static::assertNull($plan->source()->scan()->limit);
    }

    public function test_a_non_push_down_source_under_a_sink_is_left_alone(): void
    {
        $limit = NodeMother::limit(NodeMother::read(from_array([['id' => 1]])), 3);
        $plan = new LogicalPlan(new SinkMultiple(new Result($limit), new Write($limit, to_memory(new ArrayMemory()))));

        static::assertSame($plan, (new PushLimitIntoSource())->apply($plan, NodeMother::context()));
    }

    public function test_the_push_reaches_the_sinks_through_the_shared_leaf(): void
    {
        $limit = NodeMother::limit(NodeMother::scannableRead(), 3);

        $plan = (new PushLimitIntoSource())->apply(
            new LogicalPlan(new SinkMultiple(new Result($limit), new Write($limit, to_memory(new ArrayMemory())))),
            NodeMother::context(),
        );

        static::assertSame($plan->root->children()[0]->children()[0], $plan->sinkRoots()[0]->children()[0]);
    }
}
