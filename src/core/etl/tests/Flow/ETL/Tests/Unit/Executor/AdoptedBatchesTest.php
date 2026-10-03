<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Executor;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Executor\AdoptedBatches;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowTestCase;
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

        $batches = iterator_to_array((new AdoptedBatches($backend))->of($source), false);

        static::assertCount(2, $batches);
        static::assertSame(4, $backend->adopts());
    }

    public function test_a_batch_the_backend_already_owns_passes_as_the_same_instance(): void
    {
        $batch = array_to_rows([['id' => 1]], schema(int_schema('id')), new PhpBackend());

        /** @var Generator<int, Rows, null|Signal, void> $source */
        $source = (static function () use ($batch) {
            yield $batch;
        })();
        $adopted = (new AdoptedBatches(new PhpBackend()))->of($source);

        static::assertSame($batch, $adopted->current());
    }

    public function test_a_stop_reaches_the_source(): void
    {
        $extractor = new CountingExtractor(
            schema(int_schema('id')),
            array_to_rows([['id' => 1], ['id' => 2], ['id' => 3]], schema(int_schema('id'))),
        );
        $extractor->withBatchSize(1);
        $adopted = (new AdoptedBatches(new PhpBackend()))->of($extractor->extract(flow_context()));

        static::assertCount(1, $adopted->current());
        $adopted->send(Signal::STOP);

        static::assertFalse($adopted->valid());
        static::assertSame(1, $extractor->batchesYielded);
    }
}
