<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Config;
use Flow\ETL\ExternalSort\MemorySort;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Tests\Double\AllRowTypesFakeExtractor;
use Flow\ETL\Tests\Double\CacheSpy;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class SortTest extends IntegrationTestCase
{
    public function test_etl_sort_at_disk_in_memory() : void
    {
        \ini_set('memory_limit', '500M');

        $config = Config::builder()
            ->id($id = 'test_etl_sort_by_in_memory')
            ->cache($cacheSpy = new CacheSpy(Config::default()->cache()))
            ->externalSort(new MemorySort($id, $cacheSpy, Unit::fromKb(10)));

        read(new AllRowTypesFakeExtractor($rowsets = 50, $rows = 50), $config)
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

        $config = Config::builder()
            ->id($id = 'test_etl_sort_by_in_memory')
            ->cache($cacheSpy = new CacheSpy(Config::default()->cache()));

        $rows = read(new AllRowTypesFakeExtractor($rowsets = 20, $rows = 2), $config)
            ->sortBy(ref('id'))
            ->fetch();

        $cache = \array_diff(\scandir($this->cacheDir), ['..', '.']);

        $this->assertEmpty($cache);
        $this->assertSame(\range(0, 39), $rows->reduceToArray('id'));
        $this->assertSame(20, $cacheSpy->writes());
    }
}
