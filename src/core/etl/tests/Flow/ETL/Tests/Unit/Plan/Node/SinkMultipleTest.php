<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Node;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Materialization;
use Flow\ETL\Plan\Node\Result;
use Flow\ETL\Plan\Node\SinkMultiple;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Plan\Redefined;
use Flow\ETL\Plan\RowCount;
use Flow\ETL\Plan\Transparency;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

use function Flow\ETL\DSL\to_memory;

final class SinkMultipleTest extends FlowTestCase
{
    public function test_the_result_comes_first_then_every_sink_in_order(): void
    {
        $read = NodeMother::read();
        $result = new Result($read);
        $write = new Write($read, to_memory(new ArrayMemory()));
        $transaction = new Transaction(new RecordingTransaction(), new Write($read, to_memory(new ArrayMemory())));

        static::assertSame(
            [$result, $write, $transaction],
            (new SinkMultiple($result, $write, $transaction))->children(),
        );
    }

    public function test_it_needs_at_least_one_sink_beside_the_result(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('SinkMultiple needs two or more consumers');

        new SinkMultiple(new Result(NodeMother::read()));
    }

    public function test_with_children_returns_the_same_instance_when_children_are_identical(): void
    {
        $read = NodeMother::read();
        $node = new SinkMultiple(new Result($read), new Write($read, to_memory(new ArrayMemory())));

        static::assertSame($node, $node->withChildren($node->children()));
    }

    public function test_with_children_rebuilds_over_new_consumers(): void
    {
        $read = NodeMother::read();
        $node = new SinkMultiple(new Result($read), new Write($read, to_memory(new ArrayMemory())));
        $result = new Result(NodeMother::select($read));
        $write = new Write($read, to_memory(new ArrayMemory()));

        $rebuilt = $node->withChildren([$result, $write]);

        static::assertNotSame($node, $rebuilt);
        static::assertSame([$result, $write], $rebuilt->children());
    }

    public function test_with_children_rebuilds_over_a_transaction_child(): void
    {
        $read = NodeMother::read();
        $node = new SinkMultiple(new Result($read), new Write($read, to_memory(new ArrayMemory())));
        $transaction = new Transaction(new RecordingTransaction(), new Write($read, to_memory(new ArrayMemory())));

        $rebuilt = $node->withChildren([$node->children()[0], $transaction]);

        static::assertSame([$node->children()[0], $transaction], $rebuilt->children());
    }

    public function test_with_children_refuses_a_first_child_that_is_not_a_result(): void
    {
        $read = NodeMother::read();
        $node = new SinkMultiple(new Result($read), new Write($read, to_memory(new ArrayMemory())));

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'The first child of a SinkMultiple must stay a Result, ' . $read::class . ' given',
        );

        $node->withChildren([$read, new Write($read, to_memory(new ArrayMemory()))]);
    }

    public function test_with_children_refuses_a_sink_rewritten_to_a_non_sink(): void
    {
        $read = NodeMother::read();
        $node = new SinkMultiple(new Result($read), new Write($read, to_memory(new ArrayMemory())));

        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage(
            'A sink root rewrite must return a Write or a Transaction, ' . $read::class . ' given',
        );

        $node->withChildren([new Result($read), $read]);
    }

    public function test_declarations(): void
    {
        $read = NodeMother::read();
        $node = new SinkMultiple(new Result($read), new Write($read, to_memory(new ArrayMemory())));

        static::assertSame(RowCount::preserving, $node->rowCount());
        static::assertSame(Transparency::opaque, $node->transparency());
        static::assertSame(Materialization::streaming, $node->materialization());
        static::assertEquals(Redefined::none(), $node->redefines());
    }
}
