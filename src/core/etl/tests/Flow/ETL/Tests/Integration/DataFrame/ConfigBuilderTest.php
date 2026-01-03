<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{analyze, config_builder};
use Flow\ETL\Config\Cache\CacheConfig;
use Flow\ETL\Sort\SortAlgorithms;
use Flow\ETL\Tests\FlowIntegrationTestCase;

final class ConfigBuilderTest extends FlowIntegrationTestCase
{
    #[\Override]
    protected function tearDown() : void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . $this->cacheDir->path());

        parent::tearDown();
    }

    public function test_config_builder_with_analyze() : void
    {
        $analyze = analyze()->withSchema()->withColumnStatistics();

        $config = config_builder()
            ->analyze($analyze)
            ->build();

        self::assertSame($analyze, $config->analyze());
    }

    public function test_config_without_analyze_returns_null() : void
    {
        $config = config_builder()->build();

        self::assertNull($config->analyze());
    }

    public function test_creating_custom_cache_dir() : void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . __DIR__ . '/var/cache');
        $config = config_builder()->build();

        self::assertSame(
            \str_replace('\\', '/', $config->cache->localFilesystemCacheDir->path()),
            \str_replace('\\', '/', __DIR__ . '/var/cache')
        );
    }

    public function test_default_cache_dir() : void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=');
        $config = config_builder()->build();

        self::assertSame(
            \str_replace('\\', '/', sys_get_temp_dir() . '/flow_php/cache'),
            \str_replace('\\', '/', $config->cache->localFilesystemCacheDir->path())
        );
    }

    public function test_default_sorting_algorithm() : void
    {
        $config = config_builder()->build();

        self::assertSame(
            SortAlgorithms::MEMORY_FALLBACK_EXTERNAL_SORT,
            $config->sort->algorithm
        );
    }
}
