<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use Flow\Telemetry\Provider\Clock\SystemClock;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\postgresql_telemetry_config;
use function Flow\PostgreSql\DSL\postgresql_telemetry_options;
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

final class PostgreSqlTelemetryConfigTest extends TestCase
{
    public function test_config_can_be_created_with_custom_options(): void
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

        static::assertSame($tel, $config->telemetry);
        static::assertSame($clock, $config->clock);
        static::assertSame($options, $config->options);
        static::assertFalse($config->options->traceQueries);
        static::assertFalse($config->options->traceTransactions);
        static::assertFalse($config->options->collectMetrics);
        static::assertTrue($config->options->logQueries);
    }

    public function test_config_can_be_created_with_default_options(): void
    {
        $clock = new SystemClock();
        $tel = $this->createTelemetry($clock);

        $config = postgresql_telemetry_config($tel, $clock);

        static::assertSame($tel, $config->telemetry);
        static::assertSame($clock, $config->clock);
        static::assertTrue($config->options->traceQueries);
        static::assertTrue($config->options->traceTransactions);
        static::assertTrue($config->options->collectMetrics);
        static::assertFalse($config->options->logQueries);
    }

    private function createTelemetry(SystemClock $clock): \Flow\Telemetry\Telemetry
    {
        $contextStorage = memory_context_storage();

        return telemetry(
            resource(),
            tracer_provider(memory_span_processor(void_exporter()), $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_exporter()), $clock),
            logger_provider(memory_log_processor(void_exporter()), $clock, $contextStorage),
        );
    }
}
