<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Mother;

use function Flow\Filesystem\DSL\{filesystem_telemetry_config, filesystem_telemetry_options, native_local_filesystem};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_exporter};
use Flow\Filesystem\Telemetry\{FilesystemTelemetryConfig, FilesystemTelemetryOptions, TraceableFilesystem};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemoryLogProcessor, MemoryMetricProcessor, MemorySpanProcessor};
use Flow\Telemetry\Telemetry;
use Psr\Clock\ClockInterface;

final class FilesystemTelemetryConfigMother
{
    public static function create(
        MemorySpanProcessor $spanProcessor,
        ?FilesystemTelemetryOptions $options = null,
    ) : FilesystemTelemetryConfig {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $tel = telemetry(
            resource(),
            tracer_provider($spanProcessor, $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_exporter()), $clock),
            logger_provider(memory_log_processor(void_exporter()), $clock, $contextStorage),
        );

        return filesystem_telemetry_config($tel, $clock, $options ?? filesystem_telemetry_options());
    }

    public static function createTelemetry(ClockInterface $clock) : Telemetry
    {
        $contextStorage = memory_context_storage();

        return telemetry(
            resource(),
            tracer_provider(memory_span_processor(void_exporter()), $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_exporter()), $clock),
            logger_provider(memory_log_processor(void_exporter()), $clock, $contextStorage),
        );
    }

    public static function createTraceableFilesystem(
        MemorySpanProcessor $spanProcessor,
        ?FilesystemTelemetryOptions $options = null,
        ?MemoryMetricProcessor $metricProcessor = null,
        ?MemoryLogProcessor $logProcessor = null,
    ) : TraceableFilesystem {
        [$config] = self::createWithTelemetry($spanProcessor, $options, $metricProcessor, $logProcessor);

        return new TraceableFilesystem(native_local_filesystem(), $config);
    }

    /**
     * @return array{TraceableFilesystem, Telemetry}
     */
    public static function createTraceableFilesystemWithTelemetry(
        MemorySpanProcessor $spanProcessor,
        ?FilesystemTelemetryOptions $options = null,
        ?MemoryMetricProcessor $metricProcessor = null,
        ?MemoryLogProcessor $logProcessor = null,
    ) : array {
        [$config, $tel] = self::createWithTelemetry($spanProcessor, $options, $metricProcessor, $logProcessor);

        return [new TraceableFilesystem(native_local_filesystem(), $config), $tel];
    }

    /**
     * @return array{FilesystemTelemetryConfig, Telemetry}
     */
    public static function createWithTelemetry(
        MemorySpanProcessor $spanProcessor,
        ?FilesystemTelemetryOptions $options = null,
        ?MemoryMetricProcessor $metricProcessor = null,
        ?MemoryLogProcessor $logProcessor = null,
    ) : array {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $tel = telemetry(
            resource(),
            tracer_provider($spanProcessor, $clock, $contextStorage),
            meter_provider($metricProcessor ?? memory_metric_processor(void_exporter()), $clock),
            logger_provider($logProcessor ?? memory_log_processor(void_exporter()), $clock, $contextStorage),
        );

        return [filesystem_telemetry_config($tel, $clock, $options ?? filesystem_telemetry_options()), $tel];
    }
}
