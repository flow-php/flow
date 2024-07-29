<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{config_builder, df, from_cache};
use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Flow\ETL\{Extractor, FlowContext};

final class CacheTest extends IntegrationTestCase
{
    public function test_cache() : void
    {
        $spyExtractor = new class(20) implements Extractor {
            public int $extractions = 0;

            private Extractor $extractor;

            public function __construct(int $rowsets)
            {
                $this->extractor = new FakeExtractor($rowsets);
            }

            public function extract(FlowContext $context) : \Generator
            {
                $this->extractions++;

                return $this->extractor->extract($context);
            }
        };

        $cache = new InMemoryCache();

        df(config_builder()->cache($cache))
            ->read(from_cache(
                'test_etl_cache',
                $spyExtractor,
            ))
            ->cache('test_etl_cache')
            ->run();

        self::assertEquals(1, $spyExtractor->extractions);
        self::assertInstanceOf(CacheIndex::class, $cache->get('test_etl_cache'));

        df(config_builder()->cache($cache))
            ->read(from_cache(
                'test_etl_cache',
                $spyExtractor,
                clear: true
            ))
            ->run();

        self::assertEquals(1, $spyExtractor->extractions);
        self::assertFalse($cache->has('test_etl_cache'));
    }
}
