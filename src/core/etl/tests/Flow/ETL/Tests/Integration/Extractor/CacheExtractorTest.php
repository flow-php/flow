<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Extractor;

use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_cache;

final class CacheExtractorTest extends FlowIntegrationTestCase
{
    public function test_extracting_rows_from_cache(): void
    {
        $cache = new InMemoryCache();

        $index = new CacheIndex($cacheKey = 'key');
        $index->add('rows_01');
        $index->add('rows_02');
        $index->add('rows_03');

        $cache->set('rows_01', array_to_rows([['id' => 1], ['id' => 2]], flow_context(config())->entryFactory()));
        $cache->set('rows_02', array_to_rows([['id' => 3], ['id' => 4]], flow_context(config())->entryFactory()));
        $cache->set('rows_03', array_to_rows([['id' => 5]], flow_context(config())->entryFactory()));

        $cache->set('key', $index);

        $extractor = from_cache($cacheKey);

        $rows = \iterator_to_array($extractor->extract(flow_context(config_builder()->cache($cache)->build())));

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

        $cache->set('rows_01', array_to_rows([['id' => 1], ['id' => 2]], flow_context(config())->entryFactory()));
        $cache->set('rows_02', array_to_rows([['id' => 3], ['id' => 4]], flow_context(config())->entryFactory()));
        $cache->set('rows_03', array_to_rows([['id' => 5]], flow_context(config())->entryFactory()));

        $cache->set('key', $index);

        $extractor = from_cache($cacheKey)->withClearOnFinish(true);

        $rows = \iterator_to_array($extractor->extract(flow_context(config_builder()->cache($cache)->build())));

        static::assertCount(3, $rows);
        static::assertFalse($cache->has('rows_01'));
        static::assertFalse($cache->has('rows_02'));
        static::assertFalse($cache->has('rows_03'));
        static::assertFalse($cache->has('key'));
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

        $rows = \iterator_to_array($extractor->extract(flow_context(config_builder()->cache($cache)->build())));

        static::assertCount(3, $rows);
        static::assertEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            \array_merge($rows[0]->toArray(), $rows[1]->toArray(), $rows[2]->toArray()),
        );
    }
}
