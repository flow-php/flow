<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use function Flow\ETL\DSL\col;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Cache\PSRSimpleCache;
use Flow\ETL\Config;
use Flow\ETL\DSL\From;
use Flow\ETL\ExternalSort\MemorySort;
use Flow\ETL\Flow;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Tests\Double\AllRowTypesFakeExtractor;
use Flow\ETL\Tests\Double\CacheSpy;
use Symfony\Component\Cache\Adapter\ArrayAdapter;
use Symfony\Component\Cache\Psr16Cache;

final class FlowTest extends IntegrationTestCase
{
    public function test_etl_cache() : void
    {
        (new Flow())->extract(new AllRowTypesFakeExtractor($rowsets = 20, $rows = 2))
            ->cache('test_etl_cache');

        $cacheContent = \array_values(\array_diff(\scandir($this->cacheDir), ['..', '.']));

        $this->assertContains(\hash('sha256', 'test_etl_cache'), $cacheContent);
    }

    public function test_etl_psr_cache() : void
    {
        (new Flow(
            Config::builder()->cache(
                $cache = new PSRSimpleCache(
                    new Psr16Cache(
                        new ArrayAdapter()
                    ),
                )
            )->build()
        ))->extract(new AllRowTypesFakeExtractor($rowsets = 20, $rows = 2))
            ->cache('test_etl_cache');

        $cachedRows = (new Flow())
            ->read(From::cache('test_etl_cache', $cache))
            ->fetch();

        $this->assertCount($rowsets * $rows, $cachedRows);

        $cache->clear('test_etl_cache');
        $this->assertCount(0, \iterator_to_array($cache->read('test_etl_cache')));
    }

    public function test_etl_sort_at_disk_in_memory() : void
    {
        \ini_set('memory_limit', '500M');

        Flow::setUp(
            Config::builder()
                ->id($id = 'test_etl_sort_by_in_memory')
                ->cache($cacheSpy = new CacheSpy(Config::default()->cache()))
                ->externalSort(new MemorySort($id, $cacheSpy, Unit::fromMb(10)))
        )->extract(new AllRowTypesFakeExtractor($rowsets = 50, $rows = 50))
            ->sortBy(ref('id'))
            ->run();

        $cache = \array_diff(\scandir($this->cacheDir), ['..', '.']);

        $this->assertEmpty($cache);
        // 50 initial writes
        // 2500 single row writes
        // 50 merged writes
        $this->assertSame(2600, $cacheSpy->writes());
        // 1 main cache
        // 50 tmp caches
        // 1 sorted cache
        // 1 extracted cache
        $this->assertSame(53, $cacheSpy->clears());
    }

    public function test_etl_sort_by_in_memory() : void
    {
        \ini_set('memory_limit', '-1');

        $rows = Flow::setUp(
            Config::builder()
                ->id($id = 'test_etl_sort_by_in_memory')
                ->cache($cacheSpy = new CacheSpy(Config::default()->cache()))
        )->extract(new AllRowTypesFakeExtractor($rowsets = 20, $rows = 2))
            ->sortBy(col('id'))
            ->fetch();

        $cache = \array_diff(\scandir($this->cacheDir), ['..', '.']);

        $this->assertEmpty($cache);
        $this->assertSame(\range(0, 39), $rows->reduceToArray('id'));
        $this->assertSame(20, $cacheSpy->writes());
    }
}
