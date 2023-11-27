<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Cache\PSRSimpleCache;
use Flow\ETL\Config;
use Flow\ETL\DSL\From;
use Flow\ETL\Flow;
use Flow\ETL\Tests\Double\AllRowTypesFakeExtractor;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Symfony\Component\Cache\Adapter\ArrayAdapter;
use Symfony\Component\Cache\Psr16Cache;

final class CacheTest extends IntegrationTestCase
{
    public function test_cache() : void
    {
        (new Flow())->extract(new AllRowTypesFakeExtractor($rowsets = 20, $rows = 2))
            ->cache('test_etl_cache')
            ->run();

        $cacheContent = \array_values(\array_diff(\scandir($this->cacheDir), ['..', '.']));

        $this->assertContains('test_etl_cache', $cacheContent);
    }

    public function test_psr_cache() : void
    {
        Flow::setUp(
            Config::builder()->cache($cache = new PSRSimpleCache(new Psr16Cache(new ArrayAdapter())))->build()
        )->extract(new AllRowTypesFakeExtractor($rowsets = 20, $rows = 2))
            ->cache('test_etl_cache')
            ->run();

        $cachedRows = Flow::setUp(Config::builder()->cache($cache)->build())
            ->read(From::cache('test_etl_cache'))
            ->fetch();

        $this->assertCount($rowsets * $rows, $cachedRows);

        $cache->clear('test_etl_cache');
        $this->assertCount(0, \iterator_to_array($cache->read('test_etl_cache')));
    }
}
