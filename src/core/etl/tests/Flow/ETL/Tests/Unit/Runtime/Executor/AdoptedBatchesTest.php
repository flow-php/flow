<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use ArrayObject;
use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Runtime\Executor\AdoptedBatches;
use Flow\ETL\Runtime\Executor\StepRunner;
use Flow\ETL\Tests\Context\VmStackContext;
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

        VmStackContext::fillWithInts();
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

    public function test_a_stop_ends_the_source_without_sending_it_on(): void
    {
        $signals = new ArrayObject();
        $ended = new ArrayObject();
        /** @var Generator<int, Rows, null|Signal, void> $source */
        $source = (static function () use ($signals, $ended): Generator {
            try {
                $signals[] = yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                $signals[] = yield array_to_rows([['id' => 2]], schema(int_schema('id')));
            } finally {
                $ended[] = 'finally';
            }
        })();
        $adopted = (new AdoptedBatches(new PhpBackend()))->of($source);
        unset($source);
        $adopted->current();

        $adopted->send(Signal::STOP);

        static::assertFalse($adopted->valid());
        static::assertSame([], $signals->getArrayCopy());
        static::assertSame(['finally'], $ended->getArrayCopy());
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
