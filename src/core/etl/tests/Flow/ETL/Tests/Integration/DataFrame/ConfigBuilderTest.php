<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\Config\Cache\CacheConfig;
use Flow\ETL\Sort\SortAlgorithms;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Telemetry\TraceableFilesystem;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Telemetry;

use function Flow\ETL\DSL\analyze;
use function Flow\ETL\DSL\config_builder;
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

final class ConfigBuilderTest extends FlowIntegrationTestCase
{
    #[\Override]
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

        static::assertSame('custom-cache', $config->cache->filesystemProtocol);
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
            \str_replace('\\', '/', $config->cache->localFilesystemCacheDir->path()),
            \str_replace('\\', '/', __DIR__ . '/var/cache'),
        );
    }

    public function test_default_cache_dir(): void
    {
        putenv(CacheConfig::CACHE_DIR_ENV . '=');
        $config = config_builder()->build();

        static::assertSame(
            \str_replace('\\', '/', sys_get_temp_dir() . '/flow_php/cache'),
            \str_replace('\\', '/', $config->cache->localFilesystemCacheDir->path()),
        );
    }

    public function test_default_cache_filesystem_protocol_is_file(): void
    {
        $config = config_builder()->build();

        static::assertSame('file', $config->cache->filesystemProtocol);
    }

    public function test_default_external_sort_filesystem_protocol_is_file(): void
    {
        $config = config_builder()->build();

        static::assertSame('file', $config->sort->filesystemProtocol);
    }

    public function test_default_sorting_algorithm(): void
    {
        $config = config_builder()->build();

        static::assertSame(SortAlgorithms::MEMORY_FALLBACK_EXTERNAL_SORT, $config->sort->algorithm);
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
            ->withTelemetry($telemetry, telemetry_options(filesystem: filesystem_telemetry_options(traceStreams: true)))
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
                telemetry_options(filesystem: filesystem_telemetry_options(traceStreams: false, collectMetrics: false)),
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
            ->withTelemetry($telemetry, telemetry_options(filesystem: filesystem_telemetry_options(traceStreams: true)))
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
