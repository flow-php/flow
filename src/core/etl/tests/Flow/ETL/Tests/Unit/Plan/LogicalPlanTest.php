<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\Limit;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\ReplaceLeaf;
use Flow\ETL\Plan\Rewrite;
use Flow\ETL\Tests\Double\ChildlessNode;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\RenameSelectRewrite;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\to_memory;

final class LogicalPlanTest extends FlowTestCase
{
    public function test_sink_roots_are_the_outputs_children_after_the_result(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $second = new Write($read, to_memory(new ArrayMemory()));

        static::assertSame(
            [$first, $second],
            (new LogicalPlan(new Outputs(new Result($read), $first, $second)))->sinks()->all(),
        );
    }

    public function test_sink_roots_is_empty_for_a_result_root(): void
    {
        static::assertSame([], (new LogicalPlan(new Result(NodeMother::read())))->sinks()->all());
    }

    public function test_consumer_inputs_are_the_nodes_the_result_and_every_write_read(): void
    {
        $read = NodeMother::read();
        $select = NodeMother::select($read);
        $limit = NodeMother::limit($read, 5);

        $consumers = (new LogicalPlan(
            new Outputs(
                new Result($select),
                new Write($read, to_memory(new ArrayMemory())),
                new Node\Transaction(
                    new RecordingTransaction(),
                    new Write($limit, to_memory(new ArrayMemory())),
                    new Write($select, to_memory(new ArrayMemory())),
                ),
            ),
        ))->consumerInputs();

        static::assertSame([$select, $read, $limit, $select], $consumers);
    }

    public function test_sinks_on_spine_are_the_root_sinks_then_the_sinks_of_every_outputs_below(): void
    {
        $read = NodeMother::read();
        $inner = new Write($read, to_memory(new ArrayMemory()));
        $innerRoot = new Outputs(new Result($read), $inner);
        $select = NodeMother::select($innerRoot);
        $outer = new Write($select, to_memory(new ArrayMemory()));

        static::assertSame(
            [$outer, $inner],
            (new LogicalPlan(new Outputs(new Result($select), $outer)))->sinksOnSpine()->all(),
        );
    }

    public function test_sinks_on_spine_skip_the_sinks_of_a_joined_frame(): void
    {
        $right = NodeMother::read();
        $joined = new Outputs(new Result($right), new Write($right, to_memory(new ArrayMemory())));

        static::assertSame(
            [],
            NodeMother::plan(NodeMother::crossJoin(NodeMother::read(), $joined))->sinksOnSpine()->all(),
        );
    }

    public function test_sinks_stay_the_root_sinks_only(): void
    {
        $read = NodeMother::read();
        $innerRoot = new Outputs(new Result($read), new Write($read, to_memory(new ArrayMemory())));

        static::assertSame([], NodeMother::plan($innerRoot)->sinks()->all());
    }

    public function test_consumer_inputs_include_the_inputs_of_sinks_below_the_root(): void
    {
        $read = NodeMother::read();
        $limit = NodeMother::limit($read, 5);
        $innerRoot = new Outputs(new Result($read), new Write($limit, to_memory(new ArrayMemory())));
        $select = NodeMother::select($innerRoot);

        static::assertSame([$select, $limit], NodeMother::plan($select)->consumerInputs());
    }

    public function test_transform_up_with_replace_leaf_rewrites_the_leaf_and_keeps_the_spine(): void
    {
        $replacement = NodeMother::read(from_array([['id' => 2]]));
        $plan = NodeMother::plan(NodeMother::select(NodeMother::read()));

        $rewritten = $plan->transformUp(new ReplaceLeaf($plan->source(), $replacement));

        static::assertSame($replacement, $rewritten->source());
        $select = $rewritten->root->children()[0];
        static::assertInstanceOf(Node\Select::class, $select);
        static::assertSame([$replacement], $select->children());
    }

    public function test_transform_up_rewrites_a_node_shared_by_the_spine_and_a_sink_once(): void
    {
        $select = NodeMother::select(NodeMother::read());
        $plan = new LogicalPlan(
            new Outputs(new Result(NodeMother::limit($select, 5)), new Write($select, to_memory(new ArrayMemory()))),
        );

        $rewritten = $plan->transformUp(new RenameSelectRewrite());

        $spineSelect = $rewritten->root->children()[0]->children()[0]->children()[0];
        static::assertInstanceOf(Node\Select::class, $spineSelect);
        static::assertSame(['name'], $spineSelect->entries);
        static::assertSame($spineSelect, $rewritten->sinks()->all()[0]->children()[0]);
    }

    public function test_transform_up_refuses_an_outputs_consumer_rewritten_to_a_non_consumer(): void
    {
        $read = NodeMother::read();
        $plan = new LogicalPlan(new Outputs(new Result($read), new Write($read, to_memory(new ArrayMemory()))));

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'An Outputs consumer rewrite must return a Result, a Write or a Transaction, '
            . Node\Read::class
            . ' given',
        );

        $plan->transformUp(new class implements Rewrite {
            public function of(Node $node): Node
            {
                return $node instanceof Write ? $node->children()[0] : $node;
            }
        });
    }

    public function test_source_returns_the_read_leaf(): void
    {
        $read = NodeMother::read();

        static::assertSame($read, NodeMother::plan(NodeMother::limit(NodeMother::select($read), 5))->source());
    }

    public function test_source_stops_at_this_frames_read_and_does_not_descend_into_a_joined_frame(): void
    {
        $read = NodeMother::read();
        $frame = NodeMother::joinRight(NodeMother::plan(NodeMother::read()));

        static::assertSame($read, NodeMother::plan(new Node\CrossJoin($read, $frame))->source());
    }

    public function test_source_throws_when_the_row_input_chain_does_not_end_in_a_read(): void
    {
        $leaf = new ChildlessNode();

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A logical plan must end in a Read, ' . $leaf::class . ' found');

        NodeMother::plan(NodeMother::select($leaf))->source();
    }

    public function test_transform_up_rebuilds_bottom_up(): void
    {
        $read = NodeMother::read();
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::select($read), 5));

        $rewritten = $plan->transformUp(new class implements Rewrite {
            public function of(Node $node): Node
            {
                return $node instanceof Limit ? new Limit($node->children()[0], 1) : $node;
            }
        });

        static::assertNotSame($plan, $rewritten);
        $limit = $rewritten->root->children()[0];
        static::assertInstanceOf(Limit::class, $limit);
        static::assertSame(1, $limit->limit);
        static::assertSame($plan->root->children()[0]->children()[0], $limit->children()[0]);
        static::assertSame($read, $rewritten->source());
    }

    public function test_transform_up_that_changes_nothing_returns_the_same_plan(): void
    {
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::select(NodeMother::read()), 5));

        $rewritten = $plan->transformUp(new class implements Rewrite {
            public function of(Node $node): Node
            {
                return $node;
            }
        });

        static::assertSame($plan, $rewritten);
    }

    public function test_spine_is_the_chain_under_the_first_consumer(): void
    {
        $select = NodeMother::select(NodeMother::read());

        static::assertSame($select, (new LogicalPlan(new Result($select)))->spine());
        static::assertSame(
            $select,
            (new LogicalPlan(
                new Outputs(new Result($select), new Write($select, to_memory(new ArrayMemory()))),
            ))->spine(),
        );
    }

    public function test_spine_of_a_write_root_is_the_chain_that_write_reads(): void
    {
        $select = NodeMother::select(NodeMother::read());

        static::assertSame($select, (new LogicalPlan(new Write($select, to_memory(new ArrayMemory()))))->spine());
    }

    public function test_spine_throws_when_the_root_carries_no_consumer(): void
    {
        $read = NodeMother::read();

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A logical plan must have a consumer root, ' . $read::class . ' found');

        (new LogicalPlan($read))->spine();
    }

    public function test_spine_throws_when_the_first_consumer_is_a_transaction(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('The first consumer of a plan cannot be a Transaction');

        $read = NodeMother::read();

        (new LogicalPlan(
            new Transaction(new RecordingTransaction(), new Write($read, to_memory(new ArrayMemory()))),
        ))->spine();
    }

    public function test_sink_roots_of_a_write_root_are_that_write(): void
    {
        $write = new Write(NodeMother::read(), to_memory(new ArrayMemory()));

        static::assertSame([$write], (new LogicalPlan($write))->sinks()->all());
    }

    public function test_sink_roots_of_a_transaction_root_are_that_transaction(): void
    {
        $transaction = new Transaction(
            new RecordingTransaction(),
            new Write(NodeMother::read(), to_memory(new ArrayMemory())),
        );

        static::assertSame([$transaction], (new LogicalPlan($transaction))->sinks()->all());
    }

    public function test_consumer_inputs_of_a_write_root_are_the_chain_it_reads(): void
    {
        $select = NodeMother::select(NodeMother::read());

        static::assertSame(
            [$select],
            (new LogicalPlan(new Write($select, to_memory(new ArrayMemory()))))->consumerInputs(),
        );
    }

    public function test_consumer_inputs_of_a_result_and_a_write_are_the_chains_both_read(): void
    {
        $read = NodeMother::read();
        $select = NodeMother::select($read);

        static::assertSame(
            [$read, $select],
            (new LogicalPlan(
                new Outputs(new Result($read), new Write($select, to_memory(new ArrayMemory()))),
            ))->consumerInputs(),
        );
    }
}
