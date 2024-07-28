<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{config_builder, df, from_cache};
use Flow\ETL\Cache\RowsCache\PSRSimpleCache;
use Flow\ETL\Config;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Symfony\Component\Cache\Adapter\ArrayAdapter;
use Symfony\Component\Cache\Psr16Cache;

final class CacheTest extends IntegrationTestCase
{
    public function test_cache() : void
    {
        df()
            ->read(new FakeExtractor($rowsets = 20))
            ->batchSize(2)
            ->cache('test_etl_cache')
            ->run();

        $cacheContent = \array_values(\array_diff(\scandir($this->cacheDir), ['..', '.']));

        self::assertContains('test_etl_cache', $cacheContent);
    }

    public function test_psr_cache() : void
    {
        $adapter = new PSRSimpleCache(new Psr16Cache(new ArrayAdapter()));

        df(config_builder()->rowsCache($adapter)->build())
            ->read(new FakeExtractor($rowsets = 20))
            ->batchSize(2)
            ->cache('test_etl_cache')
            ->run();

        $cachedRows = df(Config::builder()->rowsCache($adapter)->build())->from(from_cache('test_etl_cache'))->fetch();

        self::assertCount($rowsets, $cachedRows);

        $adapter->clear('test_etl_cache');
        self::assertCount(0, \iterator_to_array($adapter->read('test_etl_cache')));
    }
}
