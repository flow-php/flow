<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Config;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\OutOfMemoryException;
use Flow\ETL\Sort\SortAlgorithms;
use Flow\ETL\Tests\Double\FakeExtractor;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\ref;
use function ini_set;
use function range;

final class SortTest extends FlowIntegrationTestCase
{
    public function test_etl_sort_by_external_sort(): void
    {
        ini_set('memory_limit', '500M');

        $config = Config::builder()->sortMemoryLimit(Unit::fromBytes(1))->build();

        $rows = df($config)->read(new FakeExtractor(2500))->batchSize(50)->sortBy(ref('int'))->fetch();

        static::assertSame(range(0, 2499), $rows->reduceToArray('int'));
    }

    public function test_etl_sort_by_in_memory(): void
    {
        ini_set('memory_limit', '-1');

        $rows = df()->read(new FakeExtractor(40))->batchSize(2)->sortBy(ref('int'))->fetch();

        static::assertSame(range(0, 39), $rows->reduceToArray('int'));
    }

    #[RunInSeparateProcess]
    public function test_etl_sort_falls_back_to_external_sort_when_memory_limit_is_exceeded(): void
    {
        ini_set('memory_limit', '-1');

        $config = Config::builder()->sortMemoryLimit(Unit::fromMb(2))->build();

        $rows = df($config)->read(new FakeExtractor(2500))->batchSize(50)->sortBy(ref('int'))->fetch();

        static::assertSame(range(0, 2499), $rows->reduceToArray('int'));
    }

    #[RunInSeparateProcess]
    public function test_etl_sort_in_memory_throws_when_memory_limit_is_exceeded_without_fallback(): void
    {
        ini_set('memory_limit', '-1');

        $builder = Config::builder()->sortMemoryLimit(Unit::fromMb(2));
        $builder->sort->algorithm(SortAlgorithms::MEMORY_SORT);

        $this->expectException(OutOfMemoryException::class);

        df($builder->build())->read(new FakeExtractor(2500))->batchSize(50)->sortBy(ref('int'))->fetch();
    }
}
