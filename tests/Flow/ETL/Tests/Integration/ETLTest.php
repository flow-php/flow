<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use Flow\ETL\Config;
use Flow\ETL\ETL;
use Flow\ETL\ExternalSort\MemorySort;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Row\Sort;
use Flow\ETL\Tests\Double\AllRowTypesFakeExtractor;
use Flow\ETL\Tests\Double\CacheSpy;

final class ETLTest extends IntegrationTestCase
{
    public function test_etl_cache() : void
    {
        ETL::extract(new AllRowTypesFakeExtractor($rowsets = 20, $rows = 2))
            ->cache('test_etl_cache');

        $cacheContent = \array_values(\array_diff(\scandir($this->cacheDir), ['..', '.']));

        $this->assertContains(\hash('sha256', 'test_etl_cache'), $cacheContent);
    }

    public function test_etl_sort_at_disk_in_memory() : void
    {
        \ini_set('memory_limit', '500M');

        $config = Config::builder()
            ->id($id = 'test_etl_sort_by_in_memory')
            ->cache($cacheSpy = new CacheSpy(Config::default()->cache()))
            ->externalSort(new MemorySort($id, $cacheSpy, Unit::fromMb(10)))
            ->build();

        ETL::extract(new AllRowTypesFakeExtractor($rowsets = 50, $rows = 50), $config)
            ->sortBy(Sort::asc('id'))
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

        $config = Config::builder()
            ->id($id = 'test_etl_sort_by_in_memory')
            ->cache($cacheSpy = new CacheSpy(Config::default()->cache()))
            ->build();

        $rows = ETL::extract(new AllRowTypesFakeExtractor($rowsets = 20, $rows = 2), $config)
            ->sortBy(Sort::asc('id'))
            ->fetch();

        $cache = \array_diff(\scandir($this->cacheDir), ['..', '.']);

        $this->assertEmpty($cache);
        $this->assertSame(\range(0, 39), $rows->reduceToArray('id'));
        $this->assertSame(20, $cacheSpy->writes());
    }
}
