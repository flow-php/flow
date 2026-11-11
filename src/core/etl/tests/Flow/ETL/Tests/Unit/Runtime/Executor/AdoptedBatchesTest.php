<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use ArrayObject;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Runtime\Executor\AdoptedBatches;
use Flow\ETL\Runtime\Executor\StepRunner;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Generator;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;
use PHPUnit\Framework\Attributes\TestWith;
use RuntimeException;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function ini_set;
use function iterator_to_array;

final class AdoptedBatchesTest extends FlowTestCase
{
    #[RunInSeparateProcess]
    #[TestWith(['0'])]
    #[TestWith(['1'])]
    public function test_a_failure_thrown_in_ends_a_source_suspended_in_its_finally(string $ignoreArgs): void
    {
        ini_set('zend.exception_ignore_args', $ignoreArgs);
        $ended = new ArrayObject();
        /** @var Generator<int, Rows, null|Signal, void> $source */
        $source = (static function () use ($ended): Generator {
            try {
                yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                yield array_to_rows([['id' => 2]], schema(int_schema('id')));
            } finally {
                $ended->append('finally');
            }
        })();
        $context = flow_context();
        $adopted = (new AdoptedBatches($context->backend()))->of($source);
        $adopted->current();
        $runner = new StepRunner();
        $failure = new RuntimeException('failed');
        unset($source);

        // the throw() frame's opline is never set: leaving int zvals in its slot makes a destructor read them as one
        (static function (): int {
            $a0 = 2;
            $a1 = 2;
            $a2 = 2;
            $a3 = 2;
            $a4 = 2;
            $a5 = 2;
            $a6 = 2;
            $a7 = 2;
            $a8 = 2;
            $a9 = 2;
            $b0 = 2;
            $b1 = 2;
            $b2 = 2;
            $b3 = 2;
            $b4 = 2;
            $b5 = 2;
            $b6 = 2;
            $b7 = 2;
            $b8 = 2;
            $b9 = 2;
            $c0 = 2;
            $c1 = 2;
            $c2 = 2;
            $c3 = 2;
            $c4 = 2;
            $c5 = 2;
            $c6 = 2;
            $c7 = 2;
            $c8 = 2;
            $c9 = 2;

            return (
                $a0
                + $a1
                + $a2
                + $a3
                + $a4
                + $a5
                + $a6
                + $a7
                + $a8
                + $a9
                + $b0
                + $b1
                + $b2
                + $b3
                + $b4
                + $b5
                + $b6
                + $b7
                + $b8
                + $b9
                + $c0
                + $c1
                + $c2
                + $c3
                + $c4
                + $c5
                + $c6
                + $c7
                + $c8
                + $c9
            );
        })();
        $runner->unwind($adopted, $failure, $context);

        static::assertSame(['finally'], $ended->getArrayCopy());
    }

    public function test_every_column_of_every_batch_goes_through_the_backend(): void
    {
        $backend = new SpyBackend();
        $schema = schema(int_schema('id'), str_schema('name'));
        /** @var Generator<int, Rows, null|Signal, void> $source */
        $source = (static function () use ($schema) {
            yield array_to_rows([['id' => 1, 'name' => 'a']], $schema, new PhpBackend());
            yield array_to_rows([['id' => 2, 'name' => 'b']], $schema, new PhpBackend());
        })();

        static::assertCount(2, iterator_to_array((new AdoptedBatches($backend))->of($source), false));
        static::assertSame(4, $backend->adopts());
    }

    public function test_a_batch_the_backend_already_owns_passes_as_the_same_instance(): void
    {
        $batch = array_to_rows([['id' => 1]], schema(int_schema('id')), new PhpBackend());

        /** @var Generator<int, Rows, null|Signal, void> $source */
        $source = (static function () use ($batch) {
            yield $batch;
        })();

        static::assertSame($batch, (new AdoptedBatches(new PhpBackend()))->of($source)->current());
    }

    public function test_a_stop_reaches_the_source(): void
    {
        $extractor = new CountingExtractor(
            schema(int_schema('id')),
            array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id'))),
        );
        $extractor->withBatchSize(1);
        $adopted = (new AdoptedBatches(new PhpBackend()))->of(FlowTestCase::extracted(
            $extractor,
            flow_context()->backend(),
            new Criteria(),
        ));

        $batch = $adopted->current();

        static::assertInstanceOf(Rows::class, $batch);
        static::assertCount(1, $batch);
        $adopted->send(Signal::STOP);

        static::assertFalse($adopted->valid());
        static::assertSame(1, $extractor->batchesYielded);
    }

    public function test_a_corrupted_record_passes_through_unchanged(): void
    {
        $corrupted = new CorruptedRecordException('file:///a.csv', 4, 2, 'reason', 'x');
        $rows = array_to_rows([['id' => 1]], schema(int_schema('id')), new PhpBackend());
        /** @var Generator<int, CorruptedRecordException|Rows, null|Signal, void> $source */
        $source = (static function () use ($corrupted, $rows) {
            yield $corrupted;
            yield $rows;
        })();

        $adopted = iterator_to_array((new AdoptedBatches(new PhpBackend()))->of($source), false);

        static::assertSame([$corrupted, $rows], $adopted);
    }
}
