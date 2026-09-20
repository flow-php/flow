<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Outputs;
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

final class OutputsTest extends FlowTestCase
{
    public function test_the_consumers_are_the_children_in_order(): void
    {
        $read = NodeMother::read();
        $result = new Result($read);
        $write = new Write($read, to_memory(new ArrayMemory()));
        $transaction = new Transaction(new RecordingTransaction(), new Write($read, to_memory(new ArrayMemory())));

        static::assertSame([$result, $write, $transaction], (new Outputs($result, $write, $transaction))->children());
    }

    public function test_sinks_are_the_write_and_transaction_consumers(): void
    {
        $read = NodeMother::read();
        $write = new Write($read, to_memory(new ArrayMemory()));
        $transaction = new Transaction(new RecordingTransaction(), new Write($read, to_memory(new ArrayMemory())));

        static::assertSame(
            [$write, $transaction],
            (new Outputs(new Result($read), $write, $transaction))->sinks()->all(),
        );
    }

    public function test_it_needs_two_or_more_consumers(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('Outputs needs two or more consumers');

        new Outputs(new Result(NodeMother::read()));
    }

    public function test_a_write_is_accepted_as_the_spine_consumer(): void
    {
        $read = NodeMother::read();
        $first = new Write($read, to_memory(new ArrayMemory()));
        $second = new Write($read, to_memory(new ArrayMemory()));

        static::assertSame([$first, $second], (new Outputs($first, $second))->children());
    }

    public function test_the_spine_consumer_cannot_be_a_transaction(): void
    {
        $read = NodeMother::read();

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('The first consumer of a plan cannot be a Transaction');

        new Outputs(
            new Transaction(new RecordingTransaction(), new Write($read, to_memory(new ArrayMemory()))),
            new Write($read, to_memory(new ArrayMemory())),
        );
    }

    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $read = NodeMother::read();
        $node = new Outputs(new Result($read), new Write($read, to_memory(new ArrayMemory())));

        static::assertSame($node, $node->withChildren($node->children()));
    }

    public function test_with_children_rebuilds_over_new_consumers(): void
    {
        $read = NodeMother::read();
        $node = new Outputs(new Result($read), new Write($read, to_memory(new ArrayMemory())));
        $result = new Result(NodeMother::select($read));
        $write = new Write($read, to_memory(new ArrayMemory()));

        $rebuilt = $node->withChildren([$result, $write]);

        static::assertNotSame($node, $rebuilt);
        static::assertSame([$result, $write], $rebuilt->children());
    }

    public function test_with_children_rebuilds_over_a_transaction_child(): void
    {
        $read = NodeMother::read();
        $node = new Outputs(new Result($read), new Write($read, to_memory(new ArrayMemory())));
        $transaction = new Transaction(new RecordingTransaction(), new Write($read, to_memory(new ArrayMemory())));

        $rebuilt = $node->withChildren([$node->children()[0], $transaction]);

        static::assertSame([$node->children()[0], $transaction], $rebuilt->children());
    }

    public function test_with_children_refuses_a_child_that_is_not_a_consumer(): void
    {
        $read = NodeMother::read();
        $node = new Outputs(new Result($read), new Write($read, to_memory(new ArrayMemory())));

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'An Outputs consumer rewrite must return a Result, a Write or a Transaction, ' . $read::class . ' given',
        );

        $node->withChildren([new Result($read), $read]);
    }

    public function test_declarations(): void
    {
        $read = NodeMother::read();
        $node = new Outputs(new Result($read), new Write($read, to_memory(new ArrayMemory())));

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
