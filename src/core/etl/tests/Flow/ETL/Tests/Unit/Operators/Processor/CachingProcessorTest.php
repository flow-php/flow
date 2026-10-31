<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Operators\Processor\CachingProcessor;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Rows;

use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class CachingProcessorTest extends FlowTestCase
{
    public function test_a_stop_still_completes_the_cache(): void
    {
        $cache = new MemoryRowsStorage();
        $upstream = (new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(5)))->withBatchSize(1);
        $processed = (new CachingProcessor('stop-cache', $cache))->process(
            FlowTestCase::extracted($upstream, flow_context()->backend(), new Criteria()),
            flow_context()->backend(),
        );

        static::assertTrue($processed->valid());

        $processed->send(Signal::STOP);

        static::assertFalse($processed->valid());
        static::assertSame(5, $upstream->batchesYielded);

        static::assertSame(5, $cache->rows('stop-cache')->exactly());
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'), str_schema('name'));

        static::assertEquals(
            $input,
            (new CachingProcessor('bind-cache', new MemoryRowsStorage()))->bind($input)->output,
        );
    }

    public function test_caches_batches_and_yields_them(): void
    {
        $cache = new MemoryRowsStorage();

        static::assertCount(
            2,
            iterator_to_array((new CachingProcessor('test-cache', $cache))->process(
                (static function () {
                    yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 2]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            )),
        );
        static::assertSame(
            [[['id' => 1]], [['id' => 2]]],
            array_map(
                static fn(Rows $batch): array => $batch->toArray(),
                iterator_to_array($cache->get('test-cache'), false),
            ),
        );
    }

    public function test_handles_empty_input(): void
    {
        $cache = new MemoryRowsStorage();

        static::assertCount(
            0,
            iterator_to_array((new CachingProcessor('test-cache', $cache))->process(
                (static function () {
                    yield from [];
                })(),
                flow_context()->backend(),
            )),
        );
        static::assertTrue($cache->has('test-cache'));
    }

    public function test_passes_through_when_cache_exists(): void
    {
        $cache = new MemoryRowsStorage();
        $cache->append('test-cache', array_to_rows([['id' => 9]], schema(int_schema('id'))));

        static::assertCount(
            1,
            iterator_to_array((new CachingProcessor('test-cache', $cache))->process(
                (static function () {
                    yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            )),
        );
    }

    public function test_a_failing_stream_leaves_no_entry(): void
    {
        $cache = new MemoryRowsStorage();

        try {
            iterator_to_array((new CachingProcessor('failing-cache', $cache))->process(
                (static function () {
                    yield array_to_rows([['id' => 1]], schema(int_schema('id')));

                    throw new RuntimeException('upstream failed');
                })(),
                flow_context()->backend(),
            ));
            static::fail('the upstream failure must reach the caller');
        } catch (RuntimeException $exception) {
            static::assertSame('upstream failed', $exception->getMessage());
        }

        static::assertFalse($cache->has('failing-cache'));
    }
}
