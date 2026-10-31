<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Runtime\Executor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Runtime\Executor\AdoptedBatches;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class AdoptedBatchesTest extends FlowTestCase
{
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
