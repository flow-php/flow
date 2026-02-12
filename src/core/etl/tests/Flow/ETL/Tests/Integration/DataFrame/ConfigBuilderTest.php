<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use function Flow\ETL\DSL\{analyze, config_builder, telemetry_options};
use function Flow\Filesystem\DSL\{filesystem_telemetry_options, protocol};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\ETL\Config\Cache\CacheConfig;
use Flow\ETL\Sort\SortAlgorithms;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Telemetry\TraceableFilesystem;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Telemetry;

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

    public function test_filesystems_mounted_after_telemetry_are_wrapped() : void
    {
        $telemetry = $this->createTelemetry();

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('gcs'));

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(filesystem: filesystem_telemetry_options(traceStreams: true)))
            ->mount($mockFilesystem)
            ->build();

        $filesystem = $config->fstab()->for(protocol('gcs'));

        self::assertInstanceOf(TraceableFilesystem::class, $filesystem);
    }

    public function test_with_telemetry_does_not_propagate_when_filesystem_telemetry_disabled() : void
    {
        $telemetry = $this->createTelemetry();

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(filesystem: filesystem_telemetry_options(traceStreams: false, collectMetrics: false)))
            ->build();

        $filesystems = $config->fstab()->filesystems();

        foreach ($filesystems as $filesystem) {
            self::assertNotInstanceOf(TraceableFilesystem::class, $filesystem);
        }
    }

    public function test_with_telemetry_propagates_to_filesystem_when_filesystem_telemetry_enabled() : void
    {
        $telemetry = $this->createTelemetry();

        $config = config_builder()
            ->withTelemetry($telemetry, telemetry_options(filesystem: filesystem_telemetry_options(traceStreams: true)))
            ->build();

        $filesystems = $config->fstab()->filesystems();

        self::assertNotEmpty($filesystems);

        foreach ($filesystems as $filesystem) {
            self::assertInstanceOf(TraceableFilesystem::class, $filesystem);
        }
    }

    private function createTelemetry() : Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        return telemetry(
            resource(),
            tracer_provider(memory_span_processor(void_span_exporter()), $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_metric_exporter()), $clock),
            logger_provider(memory_log_processor(void_log_exporter()), $clock, $contextStorage),
        );
    }
}
