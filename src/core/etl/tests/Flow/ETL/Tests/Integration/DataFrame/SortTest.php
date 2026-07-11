<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Config;
use Flow\ETL\Sort\SortAlgorithms;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\ref;
use function range;

final class SortTest extends FlowIntegrationTestCase
{
    public function test_etl_sort_by_external_sort(): void
    {
        $config = Config::builder()->externalSortBucketSize(100);
        $config->sort->algorithm(SortAlgorithms::EXTERNAL_SORT);

        $rows = df($config->build())->read(new FakeExtractor(2500))->batchSize(50)->sortBy(ref('int'))->fetch();

        static::assertSame(range(0, 2499), $rows->reduceToArray('int'));
    }

    public function test_etl_sort_by_in_memory(): void
    {
        $config = Config::builder();
        $config->sort->algorithm(SortAlgorithms::MEMORY_SORT);

        $rows = df($config->build())->read(new FakeExtractor(40))->batchSize(2)->sortBy(ref('int'))->fetch();

        static::assertSame(range(0, 39), $rows->reduceToArray('int'));
    }
}
