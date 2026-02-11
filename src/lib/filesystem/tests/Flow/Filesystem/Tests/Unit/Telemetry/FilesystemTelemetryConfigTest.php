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
        $tel = $this->createTelemetry();
        $options = filesystem_telemetry_options(
            traceFilesystemOperations: false,
            traceStreamOperations: true,
        );

        $config = filesystem_telemetry_config($tel, $options);

        self::assertSame($tel, $config->telemetry);
        self::assertSame($options, $config->options);
        self::assertFalse($config->options->traceFilesystemOperations);
        self::assertTrue($config->options->traceStreamOperations);
    }

    public function test_config_can_be_created_with_default_options() : void
    {
        $tel = $this->createTelemetry();

        $config = filesystem_telemetry_config($tel);

        self::assertSame($tel, $config->telemetry);
        self::assertTrue($config->options->traceFilesystemOperations);
        self::assertTrue($config->options->traceStreamOperations);
    }

    private function createTelemetry() : \Flow\Telemetry\Telemetry
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
