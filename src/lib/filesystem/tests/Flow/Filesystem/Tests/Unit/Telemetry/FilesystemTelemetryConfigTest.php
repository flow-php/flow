<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use function Flow\Filesystem\DSL\{filesystem_telemetry_config, filesystem_telemetry_options};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\Telemetry\Provider\Clock\SystemClock;
use PHPUnit\Framework\TestCase;

final class FilesystemTelemetryConfigTest extends TestCase
{
    public function test_config_can_be_created_with_custom_options() : void
    {
        $clock = new SystemClock();
        $tel = $this->createTelemetry($clock);
        $options = filesystem_telemetry_options(
            traceStreams: false,
            collectMetrics: false,
        );

        $config = filesystem_telemetry_config($tel, $clock, $options);

        self::assertSame($tel, $config->telemetry);
        self::assertSame($clock, $config->clock);
        self::assertSame($options, $config->options);
        self::assertFalse($config->options->traceStreams);
        self::assertFalse($config->options->collectMetrics);
    }

    public function test_config_can_be_created_with_default_options() : void
    {
        $clock = new SystemClock();
        $tel = $this->createTelemetry($clock);

        $config = filesystem_telemetry_config($tel, $clock);

        self::assertSame($tel, $config->telemetry);
        self::assertSame($clock, $config->clock);
        self::assertTrue($config->options->traceStreams);
        self::assertTrue($config->options->collectMetrics);
    }

    private function createTelemetry(SystemClock $clock) : \Flow\Telemetry\Telemetry
    {
        $contextStorage = memory_context_storage();

        return telemetry(
            resource(),
            tracer_provider(memory_span_processor(void_span_exporter()), $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_metric_exporter()), $clock),
            logger_provider(memory_log_processor(void_log_exporter()), $clock, $contextStorage),
        );
    }
}
