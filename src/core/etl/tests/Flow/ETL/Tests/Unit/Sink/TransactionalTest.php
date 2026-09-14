<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sink;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Node\Filter;
use Flow\ETL\Plan\Node\Transaction;
use Flow\ETL\Plan\Node\Transform;
use Flow\ETL\Plan\Node\Write;
use Flow\ETL\Sink\Branched;
use Flow\ETL\Sink\Transactional;
use Flow\ETL\Sink\Transformed;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;

use function array_map;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class TransactionalTest extends FlowTestCase
{
    public function test_every_child_becomes_a_write_of_one_transaction_sharing_the_prefix(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));
        $root = $dataFrame->cursor();
        $transaction = new RecordingTransaction();

        $roots = (new Transactional(
            $transaction,
            new Transformed(new AddRowIndexTransformer('idx', StartFrom::ZERO), to_memory(new ArrayMemory())),
            new Branched(ref('id')->equals(lit(1)), to_memory(new ArrayMemory())),
        ))->roots($dataFrame->fork());

        static::assertCount(1, $roots);
        $node = $roots[0];
        static::assertInstanceOf(Transaction::class, $node);
        static::assertSame($transaction, $node->transaction);
        static::assertCount(2, $node->children());
        $transform = $node->children()[0]->children()[0];
        $filter = $node->children()[1]->children()[0];
        static::assertInstanceOf(Transform::class, $transform);
        static::assertInstanceOf(Filter::class, $filter);
        static::assertSame([$root], $transform->children());
        static::assertSame([$root], $filter->children());
    }

    public function test_a_childs_own_write_joins_the_same_transaction(): void
    {
        $inner = to_memory(new ArrayMemory());
        $outer = to_memory(new ArrayMemory());

        $roots = (new Transactional(
            new RecordingTransaction(),
            new Transformed(new CallbackTransformation(
                static fn(DataFrame $prefix): DataFrame => $prefix->write($inner),
            ), $outer),
        ))->roots(df()->read(from_array([['id' => 1]]))->fork());

        static::assertCount(1, $roots);
        $node = $roots[0];
        static::assertInstanceOf(Transaction::class, $node);
        static::assertSame([$outer, $inner], array_map(static fn(Write $write) => $write->loader, $node->children()));
    }

    public function test_a_transaction_without_a_sink_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one loader must be provided');

        new Transactional(new RecordingTransaction());
    }

    public function test_a_transaction_inside_a_transaction_is_refused(): void
    {
        $this->expectException(InvalidLogicException::class);
        $this->expectExceptionMessage('A transaction cannot contain another transaction');

        (new Transactional(
            new RecordingTransaction(),
            new Transactional(new RecordingTransaction(), to_memory(new ArrayMemory())),
        ))->roots(df()->read(from_array([['id' => 1]]))->fork());
    }
}
