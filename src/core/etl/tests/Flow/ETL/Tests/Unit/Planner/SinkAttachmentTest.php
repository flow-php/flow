<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planner;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Executor\SinkFeed;
use Flow\ETL\Executor\TransactionalSinks;
use Flow\ETL\Loader\MemoryLoader;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\SinkAttachmentMother;

use function array_map;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_memory;

final class SinkAttachmentTest extends FlowTestCase
{
    public function test_a_bare_write_on_a_spine_node_is_that_nodes_loader_and_takes_no_id(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $loader = to_memory(new ArrayMemory());
        $write = new Write($read, $loader);
        $sinks = new Sinks($write);
        [$attachment, $onSpine] = SinkAttachmentMother::over($sinks, $read);

        $remembered = $attachment->attach($sinks, $onSpine);

        static::assertSame([$loader], $remembered[$read]);
        static::assertSame(0, $attachment->next());
    }

    public function test_two_sinks_sharing_a_two_node_path_off_the_spine_are_fed_through_one_side_pipeline(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $shared = NodeMother::limit(NodeMother::select($read), 5);
        $first = new Write($shared, to_memory(new ArrayMemory()));
        $second = new Write($shared, to_memory(new ArrayMemory()));
        $sinks = new Sinks($first, $second);
        [$attachment, $onSpine] = SinkAttachmentMother::over($sinks, $read);

        $remembered = $attachment->attach($sinks, $onSpine);

        static::assertSame([SinkFeed::class], array_map(static fn($step) => $step::class, $remembered[$read]));
        // the shared pipeline and nothing else: both loaders end it directly
        static::assertSame(1, $attachment->next());
    }

    public function test_two_sinks_sharing_only_part_of_their_path_nest_a_second_side_pipeline(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $shared = NodeMother::select($read);
        $first = new Write($shared, to_memory(new ArrayMemory()));
        $second = new Write(NodeMother::limit($shared, 5), to_memory(new ArrayMemory()));
        $sinks = new Sinks($first, $second);
        [$attachment, $onSpine] = SinkAttachmentMother::over($sinks, $read);

        $remembered = $attachment->attach($sinks, $onSpine);

        static::assertSame([SinkFeed::class], array_map(static fn($step) => $step::class, $remembered[$read]));
        // the shared pipeline over Select, plus the second sink's own pipeline over its Limit
        static::assertSame(2, $attachment->next());
    }

    public function test_two_sinks_whose_paths_part_below_the_shared_node_each_get_their_own_pipeline_under_it(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $shared = NodeMother::select($read);
        $first = new Write(NodeMother::limit($shared, 5), to_memory(new ArrayMemory()));
        $second = new Write(NodeMother::limit($shared, 3), to_memory(new ArrayMemory()));
        $sinks = new Sinks($first, $second);
        [$attachment, $onSpine] = SinkAttachmentMother::over($sinks, $read);

        $remembered = $attachment->attach($sinks, $onSpine);

        static::assertSame([SinkFeed::class], array_map(static fn($step) => $step::class, $remembered[$read]));
        // Select once, then a pipeline per Limit
        static::assertSame(3, $attachment->next());
    }

    public function test_a_transaction_is_one_step_over_its_children(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $transaction = new Transaction(
            new RecordingTransaction(),
            new Write($read, to_memory(new ArrayMemory())),
            new Write(NodeMother::select($read), to_memory(new ArrayMemory())),
        );
        $sinks = new Sinks($transaction);
        [$attachment, $onSpine] = SinkAttachmentMother::over($sinks, $read);

        $remembered = $attachment->attach($sinks, $onSpine);

        static::assertSame(
            [TransactionalSinks::class],
            array_map(static fn($step) => $step::class, $remembered[$read]),
        );
    }

    public function test_a_sink_outside_a_transaction_sharing_a_node_with_one_of_its_children_is_refused(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $shared = NodeMother::select($read);
        $transaction = new Transaction(
            new RecordingTransaction(),
            new Write($shared, to_memory(new ArrayMemory())),
            new Write(NodeMother::limit($read, 1), to_memory(new ArrayMemory())),
        );
        $sinks = new Sinks($transaction, new Write($shared, to_memory(new ArrayMemory())));
        [$attachment, $onSpine] = SinkAttachmentMother::over($sinks, $read);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A sink outside a transaction cannot share a node with one of its children');

        $attachment->attach($sinks, $onSpine);
    }

    public function test_the_children_of_one_transaction_must_attach_to_the_same_node(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $select = NodeMother::select($read);
        $transaction = new Transaction(
            new RecordingTransaction(),
            new Write($read, to_memory(new ArrayMemory())),
            new Write($select, to_memory(new ArrayMemory())),
        );
        $sinks = new Sinks($transaction);
        [$attachment, $onSpine] = SinkAttachmentMother::over($sinks, $read, $select);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('Every sink of one transaction must attach to the same node');

        $attachment->attach($sinks, $onSpine);
    }

    public function test_a_sink_sharing_no_node_with_the_spine_is_refused(): void
    {
        $read = NodeMother::read(from_array([['id' => 1]], schema(int_schema('id'))));
        $sinks = new Sinks(new Write(NodeMother::read(), to_memory(new ArrayMemory())));
        [$attachment, $onSpine] = SinkAttachmentMother::over($sinks, $read);

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A sink root shares no node with the plan: ' . MemoryLoader::class);

        $attachment->attach($sinks, $onSpine);
    }
}
