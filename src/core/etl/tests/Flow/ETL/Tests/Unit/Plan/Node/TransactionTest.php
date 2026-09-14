<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\LogicalPlan;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Read;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;

final class TransactionTest extends FlowTestCase
{
    public function test_children_are_the_writes_in_declaration_order(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $second = new Write($read, to_memory(new ArrayMemory()));

        static::assertSame(
            [$first, $second],
            (new Transaction(new RecordingTransaction(), $first, $second))->children(),
        );
    }

    public function test_a_transaction_without_a_write_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one loader must be provided');

        new Transaction(new RecordingTransaction());
    }

    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $write = new Write(NodeMother::read(), to_memory(new ArrayMemory()));
        $node = new Transaction(new RecordingTransaction(), $write);

        static::assertSame($node, $node->withChildren([$write]));
    }

    public function test_with_children_rebuilds_and_keeps_the_transaction(): void
    {
        $transaction = new RecordingTransaction();
        $node = new Transaction($transaction, new Write(NodeMother::read(), to_memory(new ArrayMemory())));
        $other = new Write(NodeMother::read(), to_memory(new ArrayMemory()));

        $rebuilt = $node->withChildren([$other]);

        static::assertNotSame($node, $rebuilt);
        static::assertSame([$other], $rebuilt->children());
        static::assertSame($transaction, $rebuilt->transaction);
    }

    public function test_with_children_refuses_a_child_that_is_not_a_write(): void
    {
        $node = new Transaction(
            new RecordingTransaction(),
            new Write(NodeMother::read(), to_memory(new ArrayMemory())),
        );

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'A sink root rewrite must return a Write or a Transaction, ' . Read::class . ' given',
        );

        $node->withChildren([NodeMother::read()]);
    }

    public function test_the_source_is_reached_through_a_transaction(): void
    {
        $read = NodeMother::read();

        static::assertSame(
            $read,
            (new LogicalPlan(new Result(
                new Transaction(
                    new RecordingTransaction(),
                    new Write(NodeMother::select($read), to_memory(new ArrayMemory())),
                ),
            )))->source(),
        );
    }

    public function test_declarations(): void
    {
        $node = new Transaction(
            new RecordingTransaction(),
            new Write(NodeMother::read(), to_memory(new ArrayMemory())),
        );

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
