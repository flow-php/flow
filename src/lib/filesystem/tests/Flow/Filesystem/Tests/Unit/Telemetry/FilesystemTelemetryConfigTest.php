<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use Flow\Filesystem\Tests\Mother\FilesystemTelemetryConfigMother;
use Flow\Telemetry\Provider\Clock\SystemClock;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\filesystem_telemetry_config;
use function Flow\Filesystem\DSL\filesystem_telemetry_options;

final class FilesystemTelemetryConfigTest extends TestCase
{
    public function test_config_can_be_created_with_custom_options(): void
    {
        $clock = new SystemClock();
        $tel = FilesystemTelemetryConfigMother::createTelemetry($clock);
        $options = filesystem_telemetry_options(traceStreams: false, collectMetrics: false);

        $config = filesystem_telemetry_config($tel, $clock, $options);

        static::assertSame($tel, $config->telemetry);
        static::assertSame($clock, $config->clock);
        static::assertSame($options, $config->options);
        static::assertFalse($config->options->traceStreams);
        static::assertFalse($config->options->collectMetrics);
    }

    public function test_config_can_be_created_with_default_options(): void
    {
        $clock = new SystemClock();
        $tel = FilesystemTelemetryConfigMother::createTelemetry($clock);

        $config = filesystem_telemetry_config($tel, $clock);

        static::assertSame($tel, $config->telemetry);
        static::assertSame($clock, $config->clock);
        static::assertTrue($config->options->traceStreams);
        static::assertTrue($config->options->collectMetrics);
    }
}
