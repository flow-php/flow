<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Planning\Logical\LogicalPlan;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Planning\Logical\Node\Limit;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Transaction;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Logical\ReplaceNodes;
use Flow\ETL\Planning\Logical\Rewrite;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\ChildlessNode;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\Double\RenameSelectRewrite;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\to_memory;

final class LogicalPlanTest extends FlowTestCase
{
    public function test_writes_are_the_outputs_children_after_the_result(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $second = new Write($read, to_memory(new ArrayMemory()));

        static::assertSame(
            [$first, $second],
            (new LogicalPlan(new Outputs(new Result($read), $first, $second)))->writes()->all(),
        );
    }

    public function test_writes_is_empty_for_a_result_root(): void
    {
        static::assertSame([], (new LogicalPlan(new Result(NodeMother::read())))->writes()->all());
    }

    public function test_consumer_inputs_are_the_nodes_the_result_and_every_write_read(): void
    {
        $read = NodeMother::read();
        $select = NodeMother::select($read);
        $limit = NodeMother::limit($read, 5);

        static::assertSame(
            [$select, $read, $limit, $select],
            (new LogicalPlan(
                new Outputs(
                    new Result($select),
                    new Write($read, to_memory(new ArrayMemory())),
                    new Node\Transaction(
                        new RecordingTransaction(),
                        new Write($limit, to_memory(new ArrayMemory())),
                        new Write($select, to_memory(new ArrayMemory())),
                    ),
                ),
            ))->consumerInputs(),
        );
    }

    public function test_writes_on_spine_are_the_root_writes_then_the_writes_of_every_outputs_below(): void
    {
        $read = NodeMother::read();
        $inner = new Write($read, to_memory(new ArrayMemory()));
        $select = NodeMother::select(new Outputs(new Result($read), $inner));
        $outer = new Write($select, to_memory(new ArrayMemory()));

        static::assertSame(
            [$outer, $inner],
            (new LogicalPlan(new Outputs(new Result($select), $outer)))->writesOnSpine()->all(),
        );
    }

    public function test_writes_on_spine_skip_the_writes_of_a_joined_frame(): void
    {
        $right = NodeMother::read();

        static::assertSame(
            [],
            NodeMother::plan(NodeMother::crossJoin(
                NodeMother::read(),
                new Outputs(new Result($right), new Write($right, to_memory(new ArrayMemory()))),
            ))->writesOnSpine()->all(),
        );
    }

    public function test_writes_stay_the_root_writes_only(): void
    {
        $read = NodeMother::read();

        static::assertSame(
            [],
            NodeMother::plan(
                new Outputs(new Result($read), new Write($read, to_memory(new ArrayMemory()))),
            )->writes()->all(),
        );
    }

    public function test_consumer_inputs_include_the_inputs_of_writes_below_the_root(): void
    {
        $read = NodeMother::read();
        $limit = NodeMother::limit($read, 5);
        $select = NodeMother::select(new Outputs(new Result($read), new Write($limit, to_memory(new ArrayMemory()))));

        static::assertSame([$select, $limit], NodeMother::plan($select)->consumerInputs());
    }

    public function test_transform_up_with_replace_nodes_rewrites_the_leaf_and_keeps_the_spine(): void
    {
        $replacement = NodeMother::read(from_array([['id' => 2]]));
        $plan = NodeMother::plan(NodeMother::select(NodeMother::read()));

        $rewritten = $plan->transformUp(ReplaceNodes::one($plan->source(), $replacement));

        static::assertSame($replacement, $rewritten->source());
        $select = $rewritten->root->children()[0];
        static::assertInstanceOf(Node\Select::class, $select);
        static::assertSame([$replacement], $select->children());
    }

    public function test_transform_up_rewrites_a_node_shared_by_the_spine_and_a_write_once(): void
    {
        $select = NodeMother::select(NodeMother::read());

        $rewritten = (new LogicalPlan(
            new Outputs(new Result(NodeMother::limit($select, 5)), new Write($select, to_memory(new ArrayMemory()))),
        ))->transformUp(new RenameSelectRewrite());

        $spineSelect = $rewritten->root->children()[0]->children()[0]->children()[0];
        static::assertInstanceOf(Node\Select::class, $spineSelect);
        static::assertSame(['name'], $spineSelect->columns);
        static::assertSame($spineSelect, $rewritten->writes()->all()[0]->children()[0]);
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
            public function of(Node $original, Node $rebuilt): Node
            {
                return $rebuilt instanceof Write ? $rebuilt->children()[0] : $rebuilt;
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

        static::assertSame(
            $read,
            NodeMother::plan(
                new Node\CrossJoin($read, NodeMother::joinRight(NodeMother::plan(NodeMother::read()))),
            )->source(),
        );
    }

    public function test_reads_are_every_read_once_a_joined_frames_included(): void
    {
        $read = NodeMother::read();
        $right = NodeMother::read();

        $reads = NodeMother::plan(
            new Node\CrossJoin(
                NodeMother::select($read),
                NodeMother::joinRight(NodeMother::plan(NodeMother::limit($right, 5))),
            ),
        )->reads();

        static::assertCount(2, $reads);
        static::assertContains($read, $reads);
        static::assertContains($right, $reads);
    }

    public function test_a_plan_whose_every_source_repeats_repeats(): void
    {
        static::assertTrue(NodeMother::plan(NodeMother::read())->isRepeatable());
    }

    public function test_a_plan_is_not_repeatable_when_a_joins_right_side_is_not(): void
    {
        static::assertFalse(
            NodeMother::plan(NodeMother::join(
                NodeMother::read(),
                NodeMother::joinRight(NodeMother::plan(NodeMother::nonRepeatableRead())),
            ))->isRepeatable(),
        );
    }

    public function test_a_plan_is_not_repeatable_when_a_cross_joins_right_side_is_not(): void
    {
        static::assertFalse(
            NodeMother::plan(NodeMother::crossJoin(
                NodeMother::read(),
                NodeMother::joinRight(NodeMother::plan(NodeMother::nonRepeatableRead())),
            ))->isRepeatable(),
        );
    }

    public function test_a_plan_whose_joins_right_side_repeats_repeats(): void
    {
        static::assertTrue(
            NodeMother::plan(NodeMother::join(
                NodeMother::read(),
                NodeMother::joinRight(NodeMother::plan(NodeMother::read())),
            ))->isRepeatable(),
        );
    }

    public function test_a_plan_whose_consumers_share_a_source_repeats(): void
    {
        $read = NodeMother::read();

        static::assertTrue(
            (new LogicalPlan(
                new Outputs(new Result($read), new Write($read, to_memory(new ArrayMemory()))),
            ))->isRepeatable(),
        );
    }

    public function test_a_plan_is_not_repeatable_when_a_write_reads_a_join_whose_right_side_is_not(): void
    {
        static::assertFalse(
            (new LogicalPlan(
                new Outputs(
                    new Result(NodeMother::read()),
                    new Write(
                        NodeMother::join(
                            NodeMother::read(),
                            NodeMother::joinRight(NodeMother::plan(NodeMother::nonRepeatableRead())),
                        ),
                        to_memory(new ArrayMemory()),
                    ),
                ),
            ))->isRepeatable(),
        );
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
            public function of(Node $original, Node $rebuilt): Node
            {
                return $rebuilt instanceof Limit ? new Limit($rebuilt->children()[0], 1) : $rebuilt;
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

        static::assertSame($plan, $plan->transformUp(new class implements Rewrite {
            public function of(Node $original, Node $rebuilt): Node
            {
                return $rebuilt;
            }
        }));
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

        (new LogicalPlan(
            new Transaction(new RecordingTransaction(), new Write(NodeMother::read(), to_memory(new ArrayMemory()))),
        ))->spine();
    }

    public function test_writes_of_a_write_root_are_that_write(): void
    {
        $write = new Write(NodeMother::read(), to_memory(new ArrayMemory()));

        static::assertSame([$write], (new LogicalPlan($write))->writes()->all());
    }

    public function test_writes_of_a_transaction_root_are_that_transaction(): void
    {
        $transaction = new Transaction(
            new RecordingTransaction(),
            new Write(NodeMother::read(), to_memory(new ArrayMemory())),
        );

        static::assertSame([$transaction], (new LogicalPlan($transaction))->writes()->all());
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
