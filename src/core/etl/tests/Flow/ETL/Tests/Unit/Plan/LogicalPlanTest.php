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
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\ReplaceLeaf;
use Flow\ETL\Plan\Rewrite;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Tests\Double\ChildlessNode;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\RenameSelectRewrite;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\to_memory;

final class LogicalPlanTest extends FlowTestCase
{
    public function test_of_without_sinks_is_a_result_over_the_cursor(): void
    {
        $read = NodeMother::read();

        $plan = LogicalPlan::of($read);

        static::assertInstanceOf(Result::class, $plan->root);
        static::assertSame([$read], $plan->root->children());
    }

    public function test_of_with_sinks_is_the_outputs_of_the_result_then_every_sink_in_order(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $second = new Write($read, to_memory(new ArrayMemory()));

        $plan = LogicalPlan::of($read, new Sinks($first, $second));

        static::assertInstanceOf(Outputs::class, $plan->root);
        $result = $plan->root->children()[0];
        static::assertInstanceOf(Result::class, $result);
        static::assertSame([$read], $result->children());
        static::assertSame([$first, $second], $plan->sinks()->all());
    }

    public function test_sink_roots_are_the_outputs_children_after_the_result(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $second = new Write($read, to_memory(new ArrayMemory()));

        static::assertSame(
            [$first, $second],
            (new LogicalPlan(new Outputs(new Result($read), new Sinks($first, $second))))->sinks()->all(),
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
                new Sinks(
                    new Write($read, to_memory(new ArrayMemory())),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($limit, to_memory(new ArrayMemory())),
                        new Write($select, to_memory(new ArrayMemory())),
                    ),
                ),
            ),
        ))->consumerInputs();

        static::assertSame([$select, $read, $limit, $select], $consumers);
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
            new Outputs(
                new Result(NodeMother::limit($select, 5)),
                new Sinks(new Write($select, to_memory(new ArrayMemory()))),
            ),
        );

        $rewritten = $plan->transformUp(new RenameSelectRewrite());

        $spineSelect = $rewritten->root->children()[0]->children()[0]->children()[0];
        static::assertInstanceOf(Node\Select::class, $spineSelect);
        static::assertSame(['name'], $spineSelect->entries);
        static::assertSame($spineSelect, $rewritten->sinks()->all()[0]->children()[0]);
    }

    public function test_transform_up_refuses_a_sink_root_rewritten_to_a_non_sink(): void
    {
        $read = NodeMother::read();
        $plan = new LogicalPlan(
            new Outputs(new Result($read), new Sinks(new Write($read, to_memory(new ArrayMemory())))),
        );

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'A sink root rewrite must return a Write or a Transaction, ' . Node\Read::class . ' given',
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

    public function test_source_stops_at_this_frames_read_and_does_not_descend_into_a_frame(): void
    {
        $read = NodeMother::read();
        $frame = NodeMother::frame(NodeMother::plan(NodeMother::read()));

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

    public function test_transform_up_preserves_identity_for_an_untouched_subtree(): void
    {
        $plan = NodeMother::plan(NodeMother::limit(NodeMother::select(NodeMother::read()), 5));

        $rewritten = $plan->transformUp(new class implements Rewrite {
            public function of(Node $node): Node
            {
                return $node;
            }
        });

        static::assertSame($plan->root, $rewritten->root);
    }

    public function test_cursor_is_the_chain_under_the_result(): void
    {
        $select = NodeMother::select(NodeMother::read());

        static::assertSame($select, LogicalPlan::of($select)->cursor());
        static::assertSame(
            $select,
            LogicalPlan::of($select, new Sinks(new Write($select, to_memory(new ArrayMemory()))))->cursor(),
        );
    }

    public function test_cursor_throws_when_the_root_is_not_a_result(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A logical plan must have a Result root');

        (new LogicalPlan(NodeMother::read()))->cursor();
    }

    public function test_with_cursor_keeps_the_sink_roots(): void
    {
        $read = NodeMother::read();
        $write = new Write($read, to_memory(new ArrayMemory()));
        $select = NodeMother::select($read);

        $plan = LogicalPlan::of($read, new Sinks($write))->withCursor($select);

        static::assertSame($select, $plan->cursor());
        static::assertSame([$write], $plan->sinks()->all());
    }

    public function test_with_sinks_appends_after_the_sink_roots_already_attached(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $second = new Write($read, to_memory(new ArrayMemory()));

        $plan = LogicalPlan::of($read, new Sinks($first))->withSinks(new Sinks($second));

        static::assertSame($read, $plan->cursor());
        static::assertSame([$first, $second], $plan->sinks()->all());
    }
}
