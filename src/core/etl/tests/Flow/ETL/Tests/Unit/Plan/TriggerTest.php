<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan;

use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Node\Count;
use Flow\ETL\Plan\Node\Outputs;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Sinks;
use Flow\ETL\Plan\Trigger;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;

final class TriggerTest extends FlowTestCase
{
    public function test_rows_without_sinks_is_a_result_over_the_root(): void
    {
        $read = NodeMother::read();

        $root = Trigger::rows->plan($read)->root;

        static::assertInstanceOf(Result::class, $root);
        static::assertSame([$read], $root->children());
    }

    public function test_rows_with_sinks_is_the_result_then_every_sink_in_order(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $second = new Write($read, to_memory(new ArrayMemory()));

        $root = Trigger::rows->plan($read, new Sinks($first, $second))->root;

        static::assertInstanceOf(Outputs::class, $root);
        static::assertInstanceOf(Result::class, $root->children()[0]);
        static::assertSame([$first, $second], [$root->children()[1], $root->children()[2]]);
    }

    public function test_count_without_sinks_is_a_result_over_a_count_of_the_root(): void
    {
        $read = NodeMother::read();

        $root = Trigger::count->plan($read)->root;

        static::assertEquals(new Result(new Count($read)), $root);
    }

    public function test_count_with_sinks_keeps_every_sink_after_the_count(): void
    {
        $read = NodeMother::read();
        $write = new Write($read, to_memory(new ArrayMemory()));

        static::assertEquals(
            new Outputs(new Result(new Count($read)), $write),
            Trigger::count->plan($read, new Sinks($write))->root,
        );
    }

    public function test_run_with_one_write_over_the_root_makes_that_write_the_root(): void
    {
        $read = NodeMother::read();
        $write = new Write($read, to_memory(new ArrayMemory()));

        static::assertSame($write, Trigger::run->plan($read, new Sinks($write))->root);
    }

    public function test_run_with_a_write_that_does_not_read_the_root_keeps_a_result_over_the_chain_end(): void
    {
        $read = NodeMother::read();
        $select = NodeMother::select($read);
        $write = new Write($read, to_memory(new ArrayMemory()));

        $root = Trigger::run->plan($select, new Sinks($write))->root;

        static::assertInstanceOf(Outputs::class, $root);
        static::assertEquals(new Result($select), $root->children()[0]);
        static::assertSame($write, $root->children()[1]);
    }

    public function test_run_with_a_transaction_only_keeps_a_result_over_the_chain_end(): void
    {
        $read = NodeMother::read();
        $transaction = new Transaction(new RecordingTransaction(), new Write($read, to_memory(new ArrayMemory())));

        $root = Trigger::run->plan($read, new Sinks($transaction))->root;

        static::assertInstanceOf(Outputs::class, $root);
        static::assertEquals(new Result($read), $root->children()[0]);
        static::assertSame($transaction, $root->children()[1]);
    }

    public function test_run_without_sinks_is_a_result_over_the_root(): void
    {
        $read = NodeMother::read();

        $root = Trigger::run->plan($read)->root;

        static::assertInstanceOf(Result::class, $root);
        static::assertSame([$read], $root->children());
    }

    public function test_run_keeps_a_result_when_only_a_later_write_reads_the_chain_end(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $select = NodeMother::select($read);
        $second = new Write($select, to_memory(new ArrayMemory()));

        $root = Trigger::run->plan($select, new Sinks($first, $second))->root;

        static::assertInstanceOf(Outputs::class, $root);
        static::assertEquals(new Result($select), $root->children()[0]);
        static::assertSame([$first, $second], [$root->children()[1], $root->children()[2]]);
    }
}
