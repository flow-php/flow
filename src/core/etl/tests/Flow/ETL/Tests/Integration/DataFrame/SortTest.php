<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{df, ref};
use Flow\ETL\Config;
use Flow\ETL\Monitoring\Memory\Unit;
use Flow\ETL\Tests\Double\{FakeExtractor};
use Flow\ETL\Tests\Integration\IntegrationTestCase;

final class SortTest extends IntegrationTestCase
{
    private static string $memoryLimit = '';

    public static function setUpBeforeClass() : void
    {
        parent::setUpBeforeClass();
        self::$memoryLimit = \ini_get('memory_limit');
    }

    public static function tearDownAfterClass() : void
    {
        parent::tearDownAfterClass();
        \ini_set('memory_limit', self::$memoryLimit);
    }

    public function test_etl_sort_by_external_sort() : void
    {
        \ini_set('memory_limit', '500M');

        $config = Config::builder()
            ->sortMemoryLimit(Unit::fromBytes(1))
            ->build();

        $rows = df($config)
            ->read(new FakeExtractor(2500))
            ->batchSize(50)
            ->sortBy(ref('int'))
            ->fetch();

        $cache = \array_diff(\scandir($this->cacheDir), ['..', '.']);

        self::assertEmpty($cache);
        self::assertSame(\range(0, 2499), $rows->reduceToArray('int'));
    }

    public function test_etl_sort_by_in_memory() : void
    {
        \ini_set('memory_limit', '-1');

        $rows = df()
            ->read(new FakeExtractor(40))
            ->batchSize(2)
            ->sortBy(ref('int'))
            ->fetch();

        $cache = \array_diff(\scandir($this->cacheDir), ['..', '.']);

        self::assertEmpty($cache);
        self::assertSame(\range(0, 39), $rows->reduceToArray('int'));
    }
}
