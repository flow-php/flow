<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Config\Cache\CacheConfig;
use Flow\ETL\Row\AdaptiveRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Sort\SortAlgorithms;
use Flow\ETL\Tests\Double\SpySerializer;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Telemetry\TraceableFilesystem;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Telemetry;
use Override;

use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\telemetry_options;
use function Flow\Filesystem\DSL\filesystem_telemetry_options;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Telemetry\DSL\logger_provider;
use function Flow\Telemetry\DSL\memory_context_storage;
use function Flow\Telemetry\DSL\memory_log_processor;
use function Flow\Telemetry\DSL\memory_metric_processor;
use function Flow\Telemetry\DSL\memory_span_processor;
use function Flow\Telemetry\DSL\meter_provider;
use function Flow\Telemetry\DSL\resource;
use function Flow\Telemetry\DSL\telemetry;
use function Flow\Telemetry\DSL\tracer_provider;
use function Flow\Telemetry\DSL\void_exporter;
use function str_replace;

final class ConfigBuilderTest extends FlowIntegrationTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . $this->cacheDir->path());

        parent::tearDown();
    }

    public function test_cache_filesystem_protocol_override(): void
    {
        $config = config_builder()
            ->mount(native_local_filesystem('custom-cache'))
            ->cacheFilesystem('custom-cache')
            ->build();

        static::assertSame('custom-cache', $config->cache->filesystemMount);
    }

    public function test_config_serializer_reaches_the_default_cache(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=' . __DIR__ . '/var/cache-serializer-mode');

        $config = config_builder()->serializer($spy = new SpySerializer())->build();

        $config->cache->cache->set(
            'key',
            $rows = rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3))),
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

    public function test_external_sort_batch_size_flows_into_cache_config(): void
    {
        $config = config_builder()->externalSortBatchSize(250)->build();

        static::assertSame(250, $config->cache->externalSortBatchSize);
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

    public function test_default_cache_filesystem_protocol_is_file(): void
    {
        $config = config_builder()->build();

        static::assertSame('file', $config->cache->filesystemMount);
    }

    public function test_default_external_sort_filesystem_protocol_is_file(): void
    {
        $config = config_builder()->build();

        static::assertSame('file', $config->sort->filesystemProtocol);
    }

    public function test_default_hydrator_is_the_adaptive_hydrator(): void
    {
        static::assertInstanceOf(AdaptiveRowHydrator::class, config_builder()->build()->hydrator());
    }

    public function test_default_sorting_algorithm(): void
    {
        $config = config_builder()->build();

        static::assertSame(SortAlgorithms::EXTERNAL_SORT, $config->sort->algorithm);
    }

    public function test_hydrator_override_wins_over_the_default(): void
    {
        $hydrator = new PhpRowHydrator();

        static::assertSame($hydrator, config_builder()->hydrator($hydrator)->build()->hydrator());
    }

    public function test_external_sort_filesystem_protocol_override(): void
    {
        $config = config_builder()->externalSortFilesystem('custom-sort')->build();

        static::assertSame('custom-sort', $config->sort->filesystemProtocol);
    }

    public function test_filesystems_mounted_after_telemetry_are_wrapped(): void
    {
        $telemetry = $this->createTelemetry();

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('gcs'));

        $config = config_builder()
            ->withTelemetry(
                $telemetry,
                telemetry_options(filesystem: filesystem_telemetry_options(trace_streams: true)),
            )
            ->mount($mockFilesystem)
            ->build();

        $filesystem = $config->fstab()->for('gcs');

        static::assertInstanceOf(TraceableFilesystem::class, $filesystem);
    }

    public function test_with_telemetry_does_not_propagate_when_filesystem_telemetry_disabled(): void
    {
        $telemetry = $this->createTelemetry();

        $config = config_builder()
            ->withTelemetry(
                $telemetry,
                telemetry_options(filesystem: filesystem_telemetry_options(
                    trace_streams: false,
                    collect_metrics: false,
                )),
            )
            ->build();

        $filesystems = $config->fstab()->filesystems();

        foreach ($filesystems as $filesystem) {
            static::assertNotInstanceOf(TraceableFilesystem::class, $filesystem);
        }
    }

    public function test_with_telemetry_propagates_to_filesystem_when_filesystem_telemetry_enabled(): void
    {
        $telemetry = $this->createTelemetry();

        $config = config_builder()
            ->withTelemetry(
                $telemetry,
                telemetry_options(filesystem: filesystem_telemetry_options(trace_streams: true)),
            )
            ->build();

        $filesystems = $config->fstab()->filesystems();

        static::assertNotEmpty($filesystems);

        foreach ($filesystems as $filesystem) {
            static::assertInstanceOf(TraceableFilesystem::class, $filesystem);
        }
    }

    private function createTelemetry(): Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        return telemetry(
            resource(),
            tracer_provider(memory_span_processor(void_exporter()), $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_exporter()), $clock),
            logger_provider(memory_log_processor(void_exporter()), $clock, $contextStorage),
        );
    }
}
