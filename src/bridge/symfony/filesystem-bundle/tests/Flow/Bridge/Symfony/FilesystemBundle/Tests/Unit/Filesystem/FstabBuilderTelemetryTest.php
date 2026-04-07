<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem;

use function Flow\Telemetry\DSL\{resource, telemetry};
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\MemoryFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\{FilesystemFactoryRegistry, FstabBuilder};
use Flow\Filesystem\Protocol;
use Flow\Filesystem\Telemetry\{FilesystemTelemetryConfig, FilesystemTelemetryOptions, TraceableFilesystem};
use Flow\Telemetry\Provider\Clock\SystemClock;
use PHPUnit\Framework\TestCase;

final class FstabBuilderTelemetryTest extends TestCase
{
    public function test_builds_table_without_telemetry_when_config_null() : void
    {
        $table = FstabBuilder::build(
            new FilesystemFactoryRegistry([new MemoryFilesystemFactory()]),
            'default',
            ['memory' => []],
        );

        self::assertNotInstanceOf(TraceableFilesystem::class, $table->for(new Protocol('memory')));
    }

    public function test_wraps_mounted_filesystems_with_traceable_when_telemetry_enabled() : void
    {
        $config = new FilesystemTelemetryConfig(telemetry(resource()), new SystemClock(), new FilesystemTelemetryOptions(true, true));

        $table = FstabBuilder::build(
            new FilesystemFactoryRegistry([new MemoryFilesystemFactory()]),
            'default',
            ['memory' => []],
            $config,
        );

        self::assertInstanceOf(TraceableFilesystem::class, $table->for(new Protocol('memory')));
    }
}
