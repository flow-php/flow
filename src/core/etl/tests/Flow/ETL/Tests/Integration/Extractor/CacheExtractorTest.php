<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Extractor;

use function Flow\ETL\DSL\{array_to_rows, config_builder, flow_context, from_array};
use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Extractor\CacheExtractor;
use Flow\ETL\Tests\Integration\FlowIntegrationTestCase;

final class CacheExtractorTest extends FlowIntegrationTestCase
{
    public function test_extracting_rows_from_cache() : void
    {
        $cache = new InMemoryCache();

        $index = new CacheIndex($cacheKey = 'key');
        $index->add('rows_01');
        $index->add('rows_02');
        $index->add('rows_03');

        $cache->set('rows_01', array_to_rows([['id' => 1], ['id' => 2]]));
        $cache->set('rows_02', array_to_rows([['id' => 3], ['id' => 4]]));
        $cache->set('rows_03', array_to_rows([['id' => 5]]));

        $cache->set('key', $index);

        $extractor = new CacheExtractor($cacheKey);

        $rows = \iterator_to_array($extractor->extract(flow_context(config_builder()->cache($cache)->build())));

        self::assertCount(3, $rows);
        self::assertTrue($cache->has('rows_01'));
        self::assertTrue($cache->has('rows_02'));
        self::assertTrue($cache->has('rows_03'));
        self::assertTrue($cache->has('key'));
    }

    public function test_extracting_rows_from_cache_with_clearing_cache_afterwards() : void
    {
        $cache = new InMemoryCache();

        $index = new CacheIndex($cacheKey = 'key');
        $index->add('rows_01');
        $index->add('rows_02');
        $index->add('rows_03');

        $cache->set('rows_01', array_to_rows([['id' => 1], ['id' => 2]]));
        $cache->set('rows_02', array_to_rows([['id' => 3], ['id' => 4]]));
        $cache->set('rows_03', array_to_rows([['id' => 5]]));

        $cache->set('key', $index);

        $extractor = (new CacheExtractor($cacheKey))->withClearOnFinish(true);

        $rows = \iterator_to_array($extractor->extract(flow_context(config_builder()->cache($cache)->build())));

        self::assertCount(3, $rows);
        self::assertFalse($cache->has('rows_01'));
        self::assertFalse($cache->has('rows_02'));
        self::assertFalse($cache->has('rows_03'));
        self::assertFalse($cache->has('key'));
    }

    public function test_fallback_extractor() : void
    {
        $cache = new InMemoryCache();

        $extractor = (new CacheExtractor('non_existing_cache_key'))
            ->withClearOnFinish(true)
            ->withFallbackExtractor(from_array([
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ]));

        $rows = \iterator_to_array($extractor->extract(flow_context(config_builder()->cache($cache)->build())));

        self::assertCount(3, $rows);
        self::assertEquals(
            [
                ['id' => 1],
                ['id' => 2],
                ['id' => 3],
            ],
            \array_merge($rows[0]->toArray(), $rows[1]->toArray(), $rows[2]->toArray())
        );
    }
}
