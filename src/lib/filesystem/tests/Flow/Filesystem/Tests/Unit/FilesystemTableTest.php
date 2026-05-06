<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use function Flow\Telemetry\DSL\{memory_span_processor, void_exporter};
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\{Filesystem, FilesystemTable, Mount};
use Flow\Filesystem\Telemetry\TraceableFilesystem;
use Flow\Filesystem\Tests\Mother\FilesystemTelemetryConfigMother;
use PHPUnit\Framework\TestCase;

final class FilesystemTableTest extends TestCase
{
    public function test_duplicate_protocol_throws() : void
    {
        $first = $this->filesystem('warehouse');
        $second = $this->filesystem('warehouse');

        $fstab = new FilesystemTable($first);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Mount 'warehouse' is already registered.");

        $fstab->mount($second);
    }

    public function test_for_resolves_by_protocol_string() : void
    {
        $fs = $this->filesystem('warehouse');
        $fstab = new FilesystemTable($fs);

        self::assertSame($fs, $fstab->for('warehouse'));
    }

    public function test_for_throws_when_protocol_not_mounted() : void
    {
        $fstab = new FilesystemTable();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filesystem with protocol missing is not mounted.');

        $fstab->for('missing');
    }

    public function test_mount_does_not_double_wrap_traceable_filesystem() : void
    {
        $fstab = new FilesystemTable();
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);

        $fs = $this->filesystem('s3');
        $traceableFs = new TraceableFilesystem($fs, $config);

        $fstab->withTelemetry($config);
        $fstab->mount($traceableFs);

        self::assertSame($traceableFs, $fstab->for('s3'));
    }

    public function test_mount_does_not_wrap_when_telemetry_not_configured() : void
    {
        $fstab = new FilesystemTable();
        $fs = $this->filesystem('azure');

        $fstab->mount($fs);

        self::assertNotInstanceOf(TraceableFilesystem::class, $fstab->for('azure'));
        self::assertSame($fs, $fstab->for('azure'));
    }

    public function test_mount_wraps_new_filesystem_when_telemetry_configured() : void
    {
        $fstab = new FilesystemTable();
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);

        $fstab->withTelemetry($config);
        $fstab->mount($this->filesystem('gcs'));

        self::assertInstanceOf(TraceableFilesystem::class, $fstab->for('gcs'));
    }

    public function test_unmount_removes_the_mount() : void
    {
        $fs = $this->filesystem('warehouse');
        $fstab = new FilesystemTable($fs);

        $fstab->unmount($fs);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filesystem with protocol warehouse is not mounted.');

        $fstab->for('warehouse');
    }

    public function test_unmount_throws_when_protocol_not_mounted() : void
    {
        $fstab = new FilesystemTable();

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filesystem with protocol missing is not mounted.');

        $fstab->unmount($this->filesystem('missing'));
    }

    public function test_with_telemetry_skips_already_traceable_filesystems() : void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);

        $traceableFs = new TraceableFilesystem($this->filesystem('sftp'), $config);
        $fstab = new FilesystemTable($traceableFs);

        $fstab->withTelemetry($config);

        self::assertSame($traceableFs, $fstab->for('sftp'));
    }

    public function test_with_telemetry_wraps_existing_filesystems_in_traceable() : void
    {
        $fstab = new FilesystemTable($this->filesystem('ftp'));
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);

        $fstab->withTelemetry($config);

        self::assertInstanceOf(TraceableFilesystem::class, $fstab->for('ftp'));
    }

    private function filesystem(string $protocol) : Filesystem
    {
        $mock = $this->createMock(Filesystem::class);
        $mock->method('mount')->willReturn(new Mount($protocol));

        return $mock;
    }
}
