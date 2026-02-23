<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use function Flow\PostgreSql\DSL\{postgresql_telemetry_config, postgresql_telemetry_options};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\Telemetry\Provider\Clock\SystemClock;
use PHPUnit\Framework\TestCase;

final class PostgreSqlTelemetryConfigTest extends TestCase
{
    public function test_config_can_be_created_with_custom_options() : void
    {
        $clock = new SystemClock();
        $tel = $this->createTelemetry($clock);
        $options = postgresql_telemetry_options(
            traceQueries: false,
            traceTransactions: false,
            collectMetrics: false,
            logQueries: true,
        );

        $config = postgresql_telemetry_config($tel, $clock, $options);

        self::assertSame($tel, $config->telemetry);
        self::assertSame($clock, $config->clock);
        self::assertSame($options, $config->options);
        self::assertFalse($config->options->traceQueries);
        self::assertFalse($config->options->traceTransactions);
        self::assertFalse($config->options->collectMetrics);
        self::assertTrue($config->options->logQueries);
    }

    public function test_config_can_be_created_with_default_options() : void
    {
        $clock = new SystemClock();
        $tel = $this->createTelemetry($clock);

        $config = postgresql_telemetry_config($tel, $clock);

        self::assertSame($tel, $config->telemetry);
        self::assertSame($clock, $config->clock);
        self::assertTrue($config->options->traceQueries);
        self::assertTrue($config->options->traceTransactions);
        self::assertTrue($config->options->collectMetrics);
        self::assertFalse($config->options->logQueries);
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
