<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\config_builder;
use Flow\ETL\Config\Cache\CacheConfig;
use Flow\ETL\Sort\SortAlgorithms;
use Flow\ETL\Tests\FlowIntegrationTestCase;

final class ConfigBuilderTest extends FlowIntegrationTestCase
{
    public function test_creating_custom_cache_dir() : void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . __DIR__ . '/var/cache');
        $config = config_builder()->build();

        self::assertSame($config->cache->localFilesystemCacheDir->path(), __DIR__ . '/var/cache');
    }

    public function test_default_cache_dir() : void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=');
        $config = config_builder()->build();

        self::assertSame(
            sys_get_temp_dir() . '/flow_php/cache',
            $config->cache->localFilesystemCacheDir->path()
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
