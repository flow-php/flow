<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\IO\Extractor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\Double\RepeatableExtractor;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\RowsMother;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_cache;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class CacheExtractorTest extends FlowIntegrationTestCase
{
    public function test_a_cached_index_declares_its_rows_exactly(): void
    {
        $cache = new MemoryRowsStorage();
        df(config_builder()->cache($cache))
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->cache('cached')
            ->run();

        $extractor = from_cache('cached', cache: $cache);

        static::assertEquals(Cardinality::exact(3), $extractor->statistics()->rows);
        static::assertSame($extractor->statistics(), $extractor->statistics());
    }

    public function test_a_cache_from_the_context_declares_nothing(): void
    {
        static::assertEquals(
            Cardinality::unknown(),
            from_cache('cached', from_array([['id' => 1]]))->statistics()->rows,
        );
    }

    public function test_a_missing_entry_without_a_fallback_declares_zero_rows(): void
    {
        static::assertEquals(
            Cardinality::exact(0),
            from_cache('missing', cache: new MemoryRowsStorage())->statistics()->rows,
        );
    }

    public function test_a_missing_entry_declares_what_the_fallback_declares(): void
    {
        static::assertEquals(
            Cardinality::exact(2),
            from_cache(
                'missing',
                from_array([['id' => 1], ['id' => 2]]),
                cache: new MemoryRowsStorage(),
            )->statistics()->rows,
        );
    }

    public function test_a_missing_entry_with_an_undeclaring_fallback_declares_nothing(): void
    {
        static::assertEquals(
            Cardinality::unknown(),
            from_cache(
                'missing',
                new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(2)),
                cache: new MemoryRowsStorage(),
            )->statistics()->rows,
        );
    }

    public function test_extracting_rows_from_cache(): void
    {
        $cache = new MemoryRowsStorage();
        $cache->append('key', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $cache->append('key', array_to_rows([['id' => 3], ['id' => 4]], schema(int_schema('id'))));
        $cache->append('key', array_to_rows([['id' => 5]], schema(int_schema('id'))));

        static::assertCount(
            3,
            iterator_to_array(FlowTestCase::extracted(
                from_cache('key')->boundTo($cache),
                flow_context()->backend(),
                new Criteria(),
            )),
        );
        static::assertTrue($cache->has('key'));
    }

    public function test_extracting_rows_from_cache_with_clearing_cache_afterwards(): void
    {
        $cache = new MemoryRowsStorage();
        $cache->append('key', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));
        $cache->append('key', array_to_rows([['id' => 3]], schema(int_schema('id'))));

        static::assertCount(
            2,
            iterator_to_array(FlowTestCase::extracted(
                from_cache('key')->withClearOnFinish(true)->boundTo($cache),
                flow_context()->backend(),
                new Criteria(),
            )),
        );
        static::assertFalse($cache->has('key'));
    }

    public function test_extracting_rows_from_filesystem_cache(): void
    {
        $cache = new FilesystemRowsStorage(
            $this->fs(),
            $this->cacheDir->suffix('/cache-extractor-streaming'),
            new PhpBackend(),
        );
        $cache->append('key', array_to_rows([
            ['id' => 1],
            ['id' => 2],
            ['id' => 3],
            ['id' => 4],
            ['id' => 5],
        ], schema(int_schema('id'))));
        $cache->close('key');

        $rows = iterator_to_array(FlowTestCase::extracted(
            from_cache('key')->boundTo($cache),
            flow_context()->backend(),
            new Criteria(),
        ));

        static::assertCount(1, $rows);
        static::assertEquals(
            [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]],
            array_merge(...array_map(static fn(Rows $batch): array => $batch->toArray(), $rows)),
        );
    }

    public function test_stop_signal_stops_extraction_and_skips_clearing(): void
    {
        $cache = new MemoryRowsStorage();
        $cache->append('key', array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $cache->append('key', array_to_rows([['id' => 2]], schema(int_schema('id'))));

        $generator = FlowTestCase::extracted(
            from_cache('key')->withClearOnFinish(true)->boundTo($cache),
            flow_context()->backend(),
            new Criteria(),
        );

        static::assertTrue($generator->valid());

        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
        static::assertTrue($cache->has('key'));
    }

    public function test_stop_signal_stops_fallback_extractor(): void
    {
        $generator = FlowTestCase::extracted(
            from_cache('missing')
                ->withFallbackExtractor(from_array([['id' => 1], ['id' => 2]]))
                ->boundTo(new MemoryRowsStorage()),
            flow_context()->backend(),
            new Criteria(),
        );

        static::assertTrue($generator->valid());

        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
    }

    public function test_fallback_extractor(): void
    {
        self::assertExtractedRowsAsArrayEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            from_cache('non_existing_cache_key')
                ->withClearOnFinish(true)
                ->withFallbackExtractor(from_array([
                    ['id' => 1],
                    ['id' => 2],
                    ['id' => 3],
                ]))
                ->boundTo(new MemoryRowsStorage()),
        );
    }

    /**
     * A cache entry that has not been written yet has no columns, which is an answer. Reaching into
     * the FlowContext for a cache would make describing a source need a running pipeline.
     */
    public function test_schema_is_not_derivable_without_a_cache_instance(): void
    {
        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage(
            'Flow\\ETL\\IO\\Extractor\\CacheExtractor cannot describe what it will produce before producing it: no cache was '
            . 'handed to from_cache(), and the one in the FlowContext is known only when the pipeline runs. Declare the '
            . 'schema with ->withSchema().',
        );

        from_cache('non_existing_cache_key')->schema();
    }

    public function test_schema_refuses_to_infer_when_the_injected_cache_does_not_hold_the_id(): void
    {
        $this->expectException(InferredSchemaException::class);
        $this->expectExceptionMessage(
            'from_cache() has nothing to infer a schema from: the cache holds no entry "non_existing_cache_key" and '
            . 'no fallback extractor is set. Declare the schema with ->withSchema(...).',
        );

        from_cache('non_existing_cache_key', cache: new MemoryRowsStorage())->schema();
    }

    public function test_a_cache_hit_is_not_matched_to_the_fallback_extractors_schema(): void
    {
        // the cache is normally written after transformations, so the fallback's shape says nothing
        // about what a hit holds
        $input = [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y']];
        $cache = new MemoryRowsStorage();

        df(config_builder()->cache($cache))->read(from_array($input))->drop(ref('b'))->cache('hit-vs-fallback')->run();

        static::assertSame(
            [['a' => 1], ['a' => 2]],
            df(config_builder()->cache($cache))
                ->read(from_cache('hit-vs-fallback', from_array($input)))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_is_repeatable_unless_it_clears_on_finish(): void
    {
        static::assertTrue(from_cache('key')->isRepeatable());
        static::assertFalse(from_cache('key')->withClearOnFinish(true)->isRepeatable());
    }

    public function test_a_missing_entry_reads_the_fallback_again_on_every_read_and_never_fills_the_cache(): void
    {
        $fallback = new CountingExtractor(schema(int_schema('id')), RowsMother::sequentialIds(2));
        $cache = new MemoryRowsStorage();
        $extractor = from_cache('key')->withFallbackExtractor($fallback)->boundTo($cache);

        iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()), false);
        iterator_to_array(FlowTestCase::extracted($extractor, flow_context()->backend(), new Criteria()), false);

        static::assertSame(2, $fallback->extractCalls);
        static::assertFalse($cache->has('key'));
    }

    public function test_is_not_repeatable_when_its_fallback_is_not(): void
    {
        static::assertFalse(from_cache('key')->withFallbackExtractor(new RepeatableExtractor(false))->isRepeatable());
        static::assertTrue(from_cache('key')->withFallbackExtractor(new RepeatableExtractor(true))->isRepeatable());
    }

    public function test_a_corrupted_record_of_the_fallback_passes_through_unchanged(): void
    {
        $batches = iterator_to_array(
            from_cache(
                'missing',
                from_array([['id' => 'x']], schema(int_schema('id'))),
                cache: new MemoryRowsStorage(),
            )->extract(flow_context()->backend(), new Criteria()),
            false,
        );

        static::assertCount(1, $batches);
        static::assertInstanceOf(CorruptedRecordException::class, $batches[0]);
    }
}
