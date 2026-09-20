<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sink;

use ArrayObject;
use Flow\ETL\DataFrame;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Plan\Format;
use Flow\ETL\Sink\Branched;
use Flow\ETL\Sink\Transactional;
use Flow\ETL\Sink\Transformed;
use Flow\ETL\Tests\Double\CallbackTransformation;
use Flow\ETL\Tests\Double\CallOrderLoader;
use Flow\ETL\Tests\Double\RecordingTransaction;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;

use function array_column;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_sequence_number;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;

final class TransactionalTest extends FlowTestCase
{
    public function test_every_child_becomes_a_write_of_one_transaction_sharing_the_prefix(): void
    {
        $transaction = new RecordingTransaction();
        $dataFrame = df()
            ->read(from_array([['id' => 1]]))
            ->write(
                new Transactional(
                    $transaction,
                    new Transformed(new AddRowIndexTransformer('idx', StartFrom::ZERO), to_memory(new ArrayMemory())),
                    new Branched(ref('id')->equals(lit(1)), to_memory(new ArrayMemory())),
                ),
            );

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque · streaming
            ├─ #2 Result  preserving · transparent · streaming
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #1 Read  source · transparent · streaming
            │        Extractor: ArrayExtractor
            └─ #7 Transaction  preserving · opaque · streaming
               ├─ #4 Write  preserving · opaque · streaming
               │  │  Loader: MemoryLoader
               │  └─ #3 Transform  unknown · opaque · streaming · redefines unknown
               │     └─ #1 Read (shared)
               └─ #6 Write  preserving · opaque · streaming
                  │  Loader: MemoryLoader
                  └─ #5 Filter  reducing · transparent · streaming
                     │  Condition: Equals
                     └─ #1 Read (shared)
            PLAN, $dataFrame->explain()->toString(format: Format::declarations));

        $dataFrame->run();

        static::assertSame(['begin', 'commit', 'begin', 'commit'], $transaction->log);
    }

    public function test_a_childs_own_write_joins_the_same_transaction(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();
        $transaction = new RecordingTransaction();

        $dataFrame = df()
            ->read(from_sequence_number('id', 0, 3))
            ->batchSize(2)
            ->write(
                new Transactional(
                    $transaction,
                    new Transformed(
                        new CallbackTransformation(static fn(DataFrame $prefix): DataFrame => $prefix->write(new CallOrderLoader(
                            'inner',
                            $log,
                        ))),
                        new CallOrderLoader('outer', $log),
                    ),
                ),
            );

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque · streaming
            ├─ #3 Result  preserving · transparent · streaming
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #2 Batch  preserving · transparent · streaming
            │     │  Batch size: 2
            │     └─ #1 Read  source · transparent · streaming
            │           Extractor: SequenceExtractor
            └─ #6 Transaction  preserving · opaque · streaming
               ├─ #4 Write  preserving · opaque · streaming
               │  │  Loader: CallOrderLoader
               │  └─ #2 Batch (shared)
               └─ #5 Write  preserving · opaque · streaming
                  │  Loader: CallOrderLoader
                  └─ #2 Batch (shared)
            PLAN, $dataFrame->explain()->toString(format: Format::declarations));

        $dataFrame->run();

        static::assertSame(['inner:2', 'outer:2', 'inner:2', 'outer:2'], $log->getArrayCopy());
        static::assertSame(['begin', 'commit', 'begin', 'commit', 'begin', 'commit'], $transaction->log);
    }

    public function test_children_run_in_write_call_order_inside_one_transaction(): void
    {
        /** @var ArrayObject<int, string> $log */
        $log = new ArrayObject();

        df()
            ->read(from_sequence_number('id', 0, 3))
            ->batchSize(2)
            ->write(
                new Transactional(
                    new RecordingTransaction(),
                    new CallOrderLoader('a', $log),
                    new CallOrderLoader('b', $log),
                ),
            )
            ->run();

        static::assertSame(['a:2', 'b:2', 'a:2', 'b:2'], $log->getArrayCopy());
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

        df()
            ->read(from_array([['id' => 1]]))
            ->write(
                new Transactional(
                    new RecordingTransaction(),
                    new Transactional(new RecordingTransaction(), to_memory(new ArrayMemory())),
                ),
            );
    }

    public function test_a_refused_transaction_leaves_the_frame_untouched(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));
        $before = $dataFrame->explain()->toString();

        try {
            $dataFrame->write(
                new Transactional(
                    new RecordingTransaction(),
                    new Transactional(new RecordingTransaction(), to_memory(new ArrayMemory())),
                ),
            );

            static::fail('Expected the nested transaction to be refused.');
        } catch (InvalidLogicException) {
        }

        static::assertSame($before, $dataFrame->explain()->toString());
    }

    public function test_a_transactional_nested_in_a_transformed_sink_commits_its_children(): void
    {
        $transaction = new RecordingTransaction();
        $memory = new ArrayMemory();

        df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->write(
                new Transformed(
                    new AddRowIndexTransformer('idx', StartFrom::ZERO),
                    new Transactional($transaction, to_memory($memory)),
                ),
            )
            ->run();

        static::assertSame(['begin', 'commit', 'begin', 'commit'], $transaction->log);
        static::assertSame([0, 1], array_column($memory->dump(), 'idx'));
    }

    public function test_getters_return_the_transaction_and_the_sinks_it_was_given(): void
    {
        $transaction = new RecordingTransaction();
        $first = to_memory(new ArrayMemory());
        $second = new Branched(lit(true), to_memory(new ArrayMemory()));

        $transactional = new Transactional($transaction, $first, $second);

        static::assertSame($transaction, $transactional->transaction());
        static::assertSame([$first, $second], $transactional->sinks());
    }

    public function test_writing_it_on_a_frame_attaches_one_transaction_root(): void
    {
        $dataFrame = df()->read(from_array([['id' => 1]]));

        (new Transactional(new RecordingTransaction(), to_memory(new ArrayMemory())))->write($dataFrame);

        static::assertSame(<<<'PLAN'
            Outputs  preserving · opaque · streaming
            ├─ #2 Result  preserving · transparent · streaming
            │  │  Rows this plan hands out: to the trigger, or to the node reading it
            │  └─ #1 Read  source · transparent · streaming
            │        Extractor: ArrayExtractor
            └─ #4 Transaction  preserving · opaque · streaming
               └─ #3 Write  preserving · opaque · streaming
                  │  Loader: MemoryLoader
                  └─ #1 Read (shared)
            PLAN, $dataFrame->explain()->toString(format: Format::declarations));
    }
}
