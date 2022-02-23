<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration;

use Flow\ETL\ETL;
use Flow\ETL\Row\Sort;
use Flow\ETL\Tests\Double\AllRowTypesFakeExtractor;

final class ETLTest extends CacheTestCase
{
    public function test_etl_cache() : void
    {
        ETL::extract(new AllRowTypesFakeExtractor(20, 2))
            ->cache('test_etl_cache');

        $cacheContent = \array_values(\array_diff(\scandir($this->cacheDir), ['..', '.']));

        $this->assertContains(\hash('sha256', 'test_etl_cache'), $cacheContent);
    }

    public function test_etl_sort_by() : void
    {
        $rows = ETL::extract(new AllRowTypesFakeExtractor(20, 2))
            ->sortBy(Sort::asc('id'))
            ->fetch();

        $cache = \array_diff(\scandir($this->cacheDir), ['..', '.']);

        $this->assertEmpty($cache);
        $this->assertSame(\range(0, 39), $rows->reduceToArray('id'));
    }
}
