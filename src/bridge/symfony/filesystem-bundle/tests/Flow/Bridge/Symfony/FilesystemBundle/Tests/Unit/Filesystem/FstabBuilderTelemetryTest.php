<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem;

use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\MemoryFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactoryRegistry;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FstabBuilder;
use Flow\Filesystem\Telemetry\FilesystemTelemetryConfig;
use Flow\Filesystem\Telemetry\FilesystemTelemetryOptions;
use Flow\Filesystem\Telemetry\TraceableFilesystem;
use Flow\Telemetry\Provider\Clock\SystemClock;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\resource;
use function Flow\Telemetry\DSL\telemetry;

final class FstabBuilderTelemetryTest extends TestCase
{
    public function test_builds_table_without_telemetry_when_config_null(): void
    {
        $table = FstabBuilder::build(
            new FilesystemFactoryRegistry([new MemoryFilesystemFactory()]),
            'default',
            ['memory' => ['type' => 'memory']],
        );

        static::assertNotInstanceOf(TraceableFilesystem::class, $table->for('memory'));
    }

    public function test_wraps_mounted_filesystems_with_traceable_when_telemetry_enabled(): void
    {
        $config = new FilesystemTelemetryConfig(
            telemetry(resource()),
            new SystemClock(),
            new FilesystemTelemetryOptions(true, true),
        );

        $table = FstabBuilder::build(
            new FilesystemFactoryRegistry([new MemoryFilesystemFactory()]),
            'default',
            ['memory' => ['type' => 'memory']],
            $config,
        );

        static::assertInstanceOf(TraceableFilesystem::class, $table->for('memory'));
    }
}
