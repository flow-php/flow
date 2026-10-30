<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Planning\Optimizer\Optimizer;
use Flow\ETL\Planning\Optimizer\Rule\PushLimitIntoSource;
use Flow\ETL\Planning\Physical\Planner;
use Flow\ETL\Runtime\CacheConfig;
use Flow\ETL\Runtime\Executor\Executor;
use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Double\RecordingLauncher;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Tests\Mother\NodeTranslatorMother;
use Flow\ETL\Values\Column\Php\ScalarColumn;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Override;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\schema;
use function Flow\Serializer\DSL\serialize_to_string;
use function Flow\Serializer\DSL\unserialize_from_string;
use function iterator_to_array;
use function putenv;
use function str_replace;

final class ConfigBuilderTest extends FlowIntegrationTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . $this->cacheDir->path());

        parent::tearDown();
    }

    public function test_there_is_no_launcher_unless_one_is_set(): void
    {
        $launcher = new RecordingLauncher();

        static::assertNull(config_builder()->build()->launcher());
        static::assertSame($launcher, config_builder()->launcher($launcher)->build()->launcher());
    }

    public function test_cache_override_takes_a_cache_object(): void
    {
        $cache = new MemoryRowsStorage();

        static::assertSame($cache, config_builder()->cache($cache)->build()->cache->cache);
    }

    public function test_the_default_cache_is_floe_files_in_the_cache_dir_on_the_configured_backend(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . __DIR__ . '/var/cache-default-storage');

        $cache = config_builder()->backend(new PhpBackend())->build()->cache->cache;
        $cache->append('key', array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id'))));

        static::assertInstanceOf(FilesystemRowsStorage::class, $cache);
        static::assertSame([[['id' => 1], ['id' => 2]], [ScalarColumn::class]], [
            array_merge(...array_map(
                static fn(Rows $batch): array => $batch->toArray(),
                iterator_to_array($cache->get('key'), false),
            )),
            array_map(
                static fn(Rows $batch): string => $batch->column('id')::class,
                iterator_to_array($cache->get('key'), false),
            ),
        ]);

        $cache->remove('key');
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

        static::assertSame($analyze, config_builder()->analyze($analyze)->build()->analyze());
    }

    public function test_config_without_analyze_returns_null(): void
    {
        static::assertNull(config_builder()->build()->analyze());
    }

    public function test_creating_custom_cache_dir(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . __DIR__ . '/var/cache');

        static::assertSame(
            str_replace('\\', '/', config_builder()->build()->cache->localFilesystemCacheDir->path()),
            str_replace('\\', '/', __DIR__ . '/var/cache'),
        );
    }

    public function test_cache_dir_override_wins_over_env(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . __DIR__ . '/var/cache-from-env');

        static::assertSame(
            str_replace('\\', '/', __DIR__ . '/var/cache-explicit'),
            str_replace(
                '\\',
                '/',
                config_builder()
                    ->cacheDir(__DIR__ . '/var/cache-explicit')
                    ->build()
                    ->cache->localFilesystemCacheDir->path(),
            ),
        );
    }

    public function test_default_cache_dir(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=');

        static::assertSame(
            str_replace('\\', '/', sys_get_temp_dir() . '/flow_php/cache'),
            str_replace('\\', '/', config_builder()->build()->cache->localFilesystemCacheDir->path()),
        );
    }

    public function test_default_cache_is_a_filesystem_cache_under_the_spill_root(): void
    {
        $config = config_builder()->build();

        static::assertInstanceOf(FilesystemRowsStorage::class, $config->cache->cache);
        static::assertSame($this->cacheDir->path(), $config->cache->localFilesystemCacheDir->path());
    }

    public function test_default_external_sort_builds_filesystem_storage(): void
    {
        $config = config_builder()->build();

        static::assertInstanceOf(ExternalSortConfig::class, $config->sort);
        static::assertInstanceOf(FilesystemRowsStorage::class, $config->sort->bucketing->storage);
    }

    public function test_default_sorting_algorithm_is_external_sort(): void
    {
        static::assertInstanceOf(ExternalSortConfig::class, config_builder()->build()->sort);
    }

    public function test_memory_sort_algorithm_override(): void
    {
        static::assertInstanceOf(MemorySortConfig::class, config_builder()->sort(memory_sort())->build()->sort);
    }

    public function test_external_sort_storage_override(): void
    {
        $storage = new MemoryRowsStorage();

        $config = config_builder()->sort(external_sort()->storage($storage))->build();

        static::assertInstanceOf(ExternalSortConfig::class, $config->sort);
        static::assertSame($storage, $config->sort->bucketing->storage);
    }

    public function test_optimizer_defaults_to_optimizer_default(): void
    {
        static::assertEquals(Optimizer::default(), config_builder()->build()->optimizer());
    }

    public function test_optimizer_can_be_overridden(): void
    {
        $optimizer = Optimizer::default()->without(PushLimitIntoSource::class);

        static::assertSame($optimizer, config_builder()->optimizer($optimizer)->build()->optimizer());
    }

    public function test_the_planner_runs_the_configured_optimizer(): void
    {
        $optimizer = Optimizer::default()->without(PushLimitIntoSource::class);

        $config = config_builder()->optimizer($optimizer)->build();

        static::assertEquals(
            new Planner($optimizer, NodeTranslatorMother::of($config), $config->backend()),
            $config->planner(),
        );
    }

    public function test_executor_defaults_to_an_executor(): void
    {
        static::assertEquals(new Executor(), config_builder()->build()->executor());
    }

    public function test_executor_can_be_overridden(): void
    {
        $executor = new Executor();

        static::assertSame($executor, config_builder()->executor($executor)->build()->executor());
    }

    public function test_the_default_serializer_follows_a_backend_set_after_a_build(): void
    {
        $builder = config_builder();
        $builder->build();
        $backend = new SpyBackend();
        $serializer = $builder->backend($backend)->build()->serializer();

        unserialize_from_string($serializer, serialize_to_string($serializer, array_to_rows([[
            'id' => 1,
        ]], schema(int_schema('id')))));

        static::assertSame(1, $backend->decodeFrames());
    }
}
