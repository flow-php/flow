<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Bucketing\Storage\FilesystemBuckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Cache\Implementation\FilesystemCache;
use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Config\Cache\CacheConfig;
use Flow\ETL\Config\Sort\ExternalSortConfig;
use Flow\ETL\Config\Sort\MemorySortConfig;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Tests\Double\SpySerializer;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Override;

use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function str_replace;

final class ConfigBuilderTest extends FlowIntegrationTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . $this->cacheDir->path());

        parent::tearDown();
    }

    public function test_cache_override_takes_a_cache_object(): void
    {
        $cache = new InMemoryCache();

        static::assertSame($cache, config_builder()->cache($cache)->build()->cache->cache);
    }

    public function test_config_serializer_reaches_the_default_cache(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . __DIR__ . '/var/cache-serializer-mode');

        $config = config_builder()->serializer($spy = new SpySerializer())->build();

        $config->cache->cache->set(
            'key',
            $rows = rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2]), row(['id' => 3])),
        );

        static::assertEquals($rows, $config->cache->cache->get('key'));
        static::assertCount(1, $spy->serialized);
        static::assertCount(1, $spy->unserialized);

        $config->cache->cache->clear();
    }

    public function test_custom_cache_is_untouched_by_config_serializer(): void
    {
        $custom = new InMemoryCache();

        $config = config_builder()->cache($custom)->serializer(new SpySerializer())->build();

        static::assertSame($custom, $config->cache->cache);
    }

    public function test_external_sort_batch_size_flows_into_sort_config(): void
    {
        $config = config_builder()->sort(external_sort()->batchSize(250))->build();

        static::assertInstanceOf(ExternalSortConfig::class, $config->sort);
        static::assertSame(250, $config->sort->bucketing->batchSize);
    }

    public function test_config_builder_with_analyze(): void
    {
        $analyze = analyze()->withSchema()->withColumnStatistics();

        $config = config_builder()->analyze($analyze)->build();

        static::assertSame($analyze, $config->analyze());
    }

    public function test_config_without_analyze_returns_null(): void
    {
        $config = config_builder()->build();

        static::assertNull($config->analyze());
    }

    public function test_creating_custom_cache_dir(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . __DIR__ . '/var/cache');
        $config = config_builder()->build();

        static::assertSame(
            str_replace('\\', '/', $config->cache->localFilesystemCacheDir->path()),
            str_replace('\\', '/', __DIR__ . '/var/cache'),
        );
    }

    public function test_cache_dir_override_wins_over_env(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . __DIR__ . '/var/cache-from-env');

        $config = config_builder()->cacheDir(__DIR__ . '/var/cache-explicit')->build();

        static::assertSame(
            str_replace('\\', '/', __DIR__ . '/var/cache-explicit'),
            str_replace('\\', '/', $config->cache->localFilesystemCacheDir->path()),
        );
    }

    public function test_default_cache_dir(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=');
        $config = config_builder()->build();

        static::assertSame(
            str_replace('\\', '/', sys_get_temp_dir() . '/flow_php/cache'),
            str_replace('\\', '/', $config->cache->localFilesystemCacheDir->path()),
        );
    }

    public function test_default_cache_is_a_filesystem_cache_under_the_spill_root(): void
    {
        $config = config_builder()->build();

        static::assertInstanceOf(FilesystemCache::class, $config->cache->cache);
        static::assertSame($this->cacheDir->path(), $config->cache->localFilesystemCacheDir->path());
    }

    public function test_default_external_sort_builds_filesystem_storage(): void
    {
        $config = config_builder()->build();

        static::assertInstanceOf(ExternalSortConfig::class, $config->sort);
        static::assertInstanceOf(FilesystemBuckets::class, $config->sort->bucketing->storage);
    }

    public function test_default_hydrator_is_the_adaptive_hydrator(): void
    {
        static::assertInstanceOf(AdaptiveRowHydrator::class, config_builder()->build()->hydrator());
    }

    public function test_default_sorting_algorithm_is_external_sort(): void
    {
        static::assertInstanceOf(ExternalSortConfig::class, config_builder()->build()->sort);
    }

    public function test_memory_sort_algorithm_override(): void
    {
        static::assertInstanceOf(MemorySortConfig::class, config_builder()->sort(memory_sort())->build()->sort);
    }

    public function test_hydrator_override_wins_over_the_default(): void
    {
        $hydrator = new PhpRowHydrator();

        static::assertSame($hydrator, config_builder()->hydrator($hydrator)->build()->hydrator());
    }

    public function test_external_sort_storage_override(): void
    {
        $storage = new MemoryBuckets();

        $config = config_builder()->sort(external_sort()->storage($storage))->build();

        static::assertInstanceOf(ExternalSortConfig::class, $config->sort);
        static::assertSame($storage, $config->sort->bucketing->storage);
    }
}
