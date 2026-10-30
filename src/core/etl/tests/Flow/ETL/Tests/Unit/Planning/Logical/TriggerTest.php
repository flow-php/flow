<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Logical;

use Flow\ETL\Planning\Logical\Node\Count;
use Flow\ETL\Planning\Logical\Node\Outputs;
use Flow\ETL\Planning\Logical\Node\Result;
use Flow\ETL\Planning\Logical\Node\Transaction;
use Flow\ETL\Planning\Logical\Node\Write;
use Flow\ETL\Planning\Logical\Trigger;
use Flow\ETL\Planning\Logical\Writes;
use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;

final class TriggerTest extends FlowTestCase
{
    public function test_rows_without_writes_is_a_result_over_the_root(): void
    {
        $read = NodeMother::read();

        $root = Trigger::rows->plan($read)->root;

        static::assertInstanceOf(Result::class, $root);
        static::assertSame([$read], $root->children());
    }

    public function test_rows_with_writes_is_the_result_then_every_write_in_order(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $second = new Write($read, to_memory(new ArrayMemory()));

        $root = Trigger::rows->plan($read, new Writes($first, $second))->root;

        static::assertInstanceOf(Outputs::class, $root);
        static::assertInstanceOf(Result::class, $root->children()[0]);
        static::assertSame([$first, $second], [$root->children()[1], $root->children()[2]]);
    }

    public function test_count_without_writes_is_a_result_over_a_count_of_the_root(): void
    {
        $read = NodeMother::read();

        static::assertEquals(new Result(new Count($read)), Trigger::count->plan($read)->root);
    }

    public function test_count_with_writes_keeps_every_write_after_the_count(): void
    {
        $read = NodeMother::read();
        $write = new Write($read, to_memory(new ArrayMemory()));

        static::assertEquals(
            new Outputs(new Result(new Count($read)), $write),
            Trigger::count->plan($read, new Writes($write))->root,
        );
    }

    public function test_run_with_one_write_over_the_root_makes_that_write_the_root(): void
    {
        $read = NodeMother::read();
        $write = new Write($read, to_memory(new ArrayMemory()));

        static::assertSame($write, Trigger::run->plan($read, new Writes($write))->root);
    }

    public function test_run_with_a_write_that_does_not_read_the_root_keeps_a_result_over_the_chain_end(): void
    {
        $read = NodeMother::read();
        $select = NodeMother::select($read);
        $write = new Write($read, to_memory(new ArrayMemory()));

        $root = Trigger::run->plan($select, new Writes($write))->root;

        static::assertInstanceOf(Outputs::class, $root);
        static::assertEquals(new Result($select), $root->children()[0]);
        static::assertSame($write, $root->children()[1]);
    }

    public function test_run_with_a_transaction_only_keeps_a_result_over_the_chain_end(): void
    {
        $read = NodeMother::read();
        $transaction = new Transaction(new RecordingTransaction(), new Write($read, to_memory(new ArrayMemory())));

        $root = Trigger::run->plan($read, new Writes($transaction))->root;

        static::assertInstanceOf(Outputs::class, $root);
        static::assertEquals(new Result($read), $root->children()[0]);
        static::assertSame($transaction, $root->children()[1]);
    }

    public function test_run_without_writes_is_a_result_over_the_root(): void
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

        $root = Trigger::run->plan($select, new Writes($first, $second))->root;

        static::assertInstanceOf(Outputs::class, $root);
        static::assertEquals(new Result($select), $root->children()[0]);
        static::assertSame([$first, $second], [$root->children()[1], $root->children()[2]]);
    }
}
