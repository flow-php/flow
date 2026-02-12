<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use function Flow\Filesystem\DSL\{filesystem_telemetry_config, filesystem_telemetry_options, protocol};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\Filesystem\{Filesystem, FilesystemTable};
use Flow\Filesystem\Telemetry\{FilesystemTelemetryConfig, TraceableFilesystem};
use Flow\Telemetry\Provider\Clock\SystemClock;
use PHPUnit\Framework\TestCase;

final class FilesystemTableTest extends TestCase
{
    public function test_mount_does_not_double_wrap_traceable_filesystem() : void
    {
        $fstab = new FilesystemTable();
        $config = $this->createTelemetryConfig();

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('s3'));

        $traceableFs = new TraceableFilesystem($mockFilesystem, $config);

        $fstab->withTelemetry($config);
        $fstab->mount($traceableFs);

        $filesystem = $fstab->for(protocol('s3'));

        self::assertInstanceOf(TraceableFilesystem::class, $filesystem);
        self::assertSame($traceableFs, $filesystem);
    }

    public function test_mount_does_not_wrap_when_telemetry_not_configured() : void
    {
        $fstab = new FilesystemTable();

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('azure'));

        $fstab->mount($mockFilesystem);

        $filesystem = $fstab->for(protocol('azure'));

        self::assertNotInstanceOf(TraceableFilesystem::class, $filesystem);
        self::assertSame($mockFilesystem, $filesystem);
    }

    public function test_mount_wraps_new_filesystem_when_telemetry_configured() : void
    {
        $fstab = new FilesystemTable();
        $config = $this->createTelemetryConfig();

        $fstab->withTelemetry($config);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('gcs'));

        $fstab->mount($mockFilesystem);

        $filesystem = $fstab->for(protocol('gcs'));

        self::assertInstanceOf(TraceableFilesystem::class, $filesystem);
    }

    public function test_with_telemetry_skips_already_traceable_filesystems() : void
    {
        $config = $this->createTelemetryConfig();

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('sftp'));

        $traceableFs = new TraceableFilesystem($mockFilesystem, $config);
        $fstab = new FilesystemTable($traceableFs);

        $fstab->withTelemetry($config);

        $filesystem = $fstab->for(protocol('sftp'));

        self::assertInstanceOf(TraceableFilesystem::class, $filesystem);
        self::assertSame($traceableFs, $filesystem);
    }

    public function test_with_telemetry_wraps_existing_filesystems_in_traceable() : void
    {
        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('ftp'));

        $fstab = new FilesystemTable($mockFilesystem);
        $config = $this->createTelemetryConfig();

        $fstab->withTelemetry($config);

        $filesystem = $fstab->for(protocol('ftp'));

        self::assertInstanceOf(TraceableFilesystem::class, $filesystem);
    }

    private function createTelemetryConfig() : FilesystemTelemetryConfig
    {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $tel = telemetry(
            resource(),
            tracer_provider(memory_span_processor(void_span_exporter()), $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_metric_exporter()), $clock),
            logger_provider(memory_log_processor(void_log_exporter()), $clock, $contextStorage),
        );

        return filesystem_telemetry_config($tel, $clock, filesystem_telemetry_options());
    }
}
