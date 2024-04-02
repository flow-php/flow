<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{df, ref};
use Flow\ETL\Config;
use Flow\ETL\ExternalSort\MemorySort;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Tests\Double\{CacheSpy, FakeExtractor};
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

        df($config)
            ->read(new FakeExtractor($rowsets = 2500))
            ->batchSize(50)
            ->sortBy(ref('int'))
            ->run();

        $cache = \array_diff(\scandir($this->cacheDir), ['..', '.']);

        self::assertEmpty($cache);
        self::assertSame(2506, $cacheSpy->writes());
        self::assertSame(5, $cacheSpy->clears());
    }

    public function test_etl_sort_by_in_memory() : void
    {
        \ini_set('memory_limit', '-1');

        $config = Config::builder()
            ->id($id = 'test_etl_sort_by_in_memory')
            ->cache($cacheSpy = new CacheSpy(Config::default()->cache()));

        $rows = df($config)
            ->read(new FakeExtractor($rowsets = 40))
            ->batchSize(2)
            ->sortBy(ref('int'))
            ->fetch();

        $cache = \array_diff(\scandir($this->cacheDir), ['..', '.']);

        self::assertEmpty($cache);
        self::assertSame(\range(0, 39), $rows->reduceToArray('int'));
        self::assertSame(1, $cacheSpy->writes());
    }
}
