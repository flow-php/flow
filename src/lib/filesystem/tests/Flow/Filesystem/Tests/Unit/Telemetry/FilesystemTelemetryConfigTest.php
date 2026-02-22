<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use function Flow\Filesystem\DSL\{filesystem_telemetry_config, filesystem_telemetry_options};
use Flow\Filesystem\Tests\Mother\FilesystemTelemetryConfigMother;
use Flow\Telemetry\Provider\Clock\SystemClock;
use PHPUnit\Framework\TestCase;

final class FilesystemTelemetryConfigTest extends TestCase
{
    public function test_config_can_be_created_with_custom_options() : void
    {
        $clock = new SystemClock();
        $tel = FilesystemTelemetryConfigMother::createTelemetry($clock);
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
        $tel = FilesystemTelemetryConfigMother::createTelemetry($clock);

        $config = filesystem_telemetry_config($tel, $clock);

        self::assertSame($tel, $config->telemetry);
        self::assertSame($clock, $config->clock);
        self::assertTrue($config->options->traceStreams);
        self::assertTrue($config->options->collectMetrics);
    }
}
