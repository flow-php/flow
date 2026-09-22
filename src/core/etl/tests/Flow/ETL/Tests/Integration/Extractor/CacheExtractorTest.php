<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Extractor;

use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Cache\Implementation\FilesystemCache;
use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\CountingCache;
use Flow\ETL\Tests\Double\CountingExtractor;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Tests\Mother\RowsMother;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_cache;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class CacheExtractorTest extends FlowIntegrationTestCase
{
    public function test_a_cached_index_declares_its_rows_exactly(): void
    {
        $cache = new InMemoryCache();
        df(config_builder()->cache($cache))
            ->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))
            ->cache('cached')
            ->run();

        $extractor = from_cache('cached', cache: $cache);

        static::assertEquals(Cardinality::exact(3), $extractor->statistics()->rows);
        static::assertSame($extractor->statistics(), $extractor->statistics());
    }

    public function test_schema_and_statistics_load_the_index_once(): void
    {
        $cache = new CountingCache(new InMemoryCache());
        df(config_builder()->cache($cache))
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->cache('cached')
            ->run();
        $loads = $cache->getCalls;

        $extractor = from_cache('cached', cache: $cache);
        $extractor->schema();
        $extractor->statistics();

        static::assertSame($loads + 1, $cache->getCalls);
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
            from_cache('missing', cache: new InMemoryCache())->statistics()->rows,
        );
    }

    public function test_a_missing_entry_declares_what_the_fallback_declares(): void
    {
        static::assertEquals(
            Cardinality::exact(2),
            from_cache(
                'missing',
                from_array([['id' => 1], ['id' => 2]]),
                cache: new InMemoryCache(),
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
                cache: new InMemoryCache(),
            )->statistics()->rows,
        );
    }

    public function test_extracting_rows_from_cache(): void
    {
        $cache = new InMemoryCache();

        $index = new CacheIndex($cacheKey = 'key');
        $index->add('rows_01');
        $index->add('rows_02');
        $index->add('rows_03');

        $cache->set('rows_01', array_to_rows(
            [['id' => 1], ['id' => 2]],
            schema(int_schema('id')),
            flow_context(config())->hydrator(),
        ));
        $cache->set('rows_02', array_to_rows(
            [['id' => 3], ['id' => 4]],
            schema(int_schema('id')),
            flow_context(config())->hydrator(),
        ));
        $cache->set('rows_03', array_to_rows(
            [['id' => 5]],
            schema(int_schema('id')),
            flow_context(config())->hydrator(),
        ));

        $cache->set('key', $index->toRows());

        $extractor = from_cache($cacheKey);

        $rows = iterator_to_array($extractor->extract(flow_context(config_builder()->cache($cache)->build())));

        static::assertCount(3, $rows);
        static::assertTrue($cache->has('rows_01'));
        static::assertTrue($cache->has('rows_02'));
        static::assertTrue($cache->has('rows_03'));
        static::assertTrue($cache->has('key'));
    }

    public function test_extracting_rows_from_cache_with_clearing_cache_afterwards(): void
    {
        $cache = new InMemoryCache();

        $index = new CacheIndex($cacheKey = 'key');
        $index->add('rows_01');
        $index->add('rows_02');
        $index->add('rows_03');

        $cache->set('rows_01', array_to_rows(
            [['id' => 1], ['id' => 2]],
            schema(int_schema('id')),
            flow_context(config())->hydrator(),
        ));
        $cache->set('rows_02', array_to_rows(
            [['id' => 3], ['id' => 4]],
            schema(int_schema('id')),
            flow_context(config())->hydrator(),
        ));
        $cache->set('rows_03', array_to_rows(
            [['id' => 5]],
            schema(int_schema('id')),
            flow_context(config())->hydrator(),
        ));

        $cache->set('key', $index->toRows());

        $extractor = from_cache($cacheKey)->withClearOnFinish(true);

        $rows = iterator_to_array($extractor->extract(flow_context(config_builder()->cache($cache)->build())));

        static::assertCount(3, $rows);
        static::assertFalse($cache->has('rows_01'));
        static::assertFalse($cache->has('rows_02'));
        static::assertFalse($cache->has('rows_03'));
        static::assertFalse($cache->has('key'));
    }

    public function test_extracting_rows_from_filesystem_cache(): void
    {
        $cache = new FilesystemCache($this->fs(), path(__DIR__ . '/var/cache-extractor-streaming'));
        $cache->clear();

        $index = new CacheIndex($cacheKey = 'key');
        $index->add('rows_01');

        $cache->set('rows_01', array_to_rows(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ],
            schema(int_schema('id')),
            flow_context(config())->hydrator(),
        ));
        $cache->set('key', $index->toRows());

        $extractor = from_cache($cacheKey);

        $rows = iterator_to_array($extractor->extract(flow_context(config_builder()->cache($cache)->build())));

        static::assertCount(1, $rows);
        static::assertEquals(
            [['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]],
            array_merge(...array_map(static fn(Rows $batch): array => $batch->toArray(), $rows)),
        );

        $cache->clear();
    }

    public function test_stop_signal_stops_extraction_and_skips_clearing(): void
    {
        $cache = new FilesystemCache($this->fs(), path(__DIR__ . '/var/cache-extractor-streaming-stop'));
        $cache->clear();

        $index = new CacheIndex($cacheKey = 'key');
        $index->add('rows_01');

        $cache->set('rows_01', array_to_rows(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
                ['id' => 4],
                ['id' => 5],
            ],
            schema(int_schema('id')),
            flow_context(config())->hydrator(),
        ));
        $cache->set('key', $index->toRows());

        $generator = from_cache($cacheKey)
            ->withClearOnFinish(true)
            ->extract(flow_context(config_builder()->cache($cache)->build()));

        static::assertTrue($generator->valid());

        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
        static::assertTrue($cache->has('rows_01'));
        static::assertTrue($cache->has('key'));

        $cache->clear();
    }

    public function test_stop_signal_stops_fallback_extractor(): void
    {
        $generator = from_cache('missing')
            ->withFallbackExtractor(from_array([['id' => 1], ['id' => 2]]))
            ->extract(flow_context(config_builder()->cache(new InMemoryCache())->build()));

        static::assertTrue($generator->valid());

        $generator->send(Signal::STOP);

        static::assertFalse($generator->valid());
    }

    public function test_fallback_extractor(): void
    {
        $cache = new InMemoryCache();

        $extractor = from_cache('non_existing_cache_key')
            ->withClearOnFinish(true)
            ->withFallbackExtractor(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ]));

        self::assertExtractedRowsAsArrayEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            $extractor,
            flow_context(config_builder()->cache($cache)->build()),
        );
    }

    /**
     * A cache entry that has not been written yet has no columns, which is an answer. Reaching into
     * the FlowContext for a cache would make describing a source need a running pipeline.
     */
    public function test_schema_returns_empty_schema_on_cache_id_miss(): void
    {
        static::assertEquals(schema(), from_cache('non_existing_cache_key')->schema());
    }

    public function test_schema_returns_empty_schema_when_the_injected_cache_does_not_hold_the_id(): void
    {
        static::assertEquals(schema(), from_cache('non_existing_cache_key', cache: new InMemoryCache())->schema());
    }

    public function test_a_cache_hit_is_not_matched_to_the_fallback_extractors_schema(): void
    {
        // the cache is normally written after transformations, so the fallback's shape says nothing
        // about what a hit holds
        $input = [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => 'y']];
        $cache = new InMemoryCache();

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
}
