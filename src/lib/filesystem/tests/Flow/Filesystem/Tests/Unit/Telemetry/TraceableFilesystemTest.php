<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use function Flow\Filesystem\DSL\{filesystem_telemetry_options, path, protocol};
use function Flow\Telemetry\DSL\{memory_span_processor, void_span_exporter};
use Flow\Filesystem\{DestinationStream, FileStatus, Filesystem, Path, SourceStream};
use Flow\Filesystem\Telemetry\{TraceableDestinationStream, TraceableFilesystem, TraceableSourceStream};
use Flow\Filesystem\Tests\Mother\FilesystemTelemetryConfigMother;
use PHPUnit\Framework\TestCase;

final class TraceableFilesystemTest extends TestCase
{
    public function test_all_telemetry_disabled_does_not_wrap_streams() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor, filesystem_telemetry_options(
            traceStreams: false,
            collectMetrics: false,
        ));
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('readFrom')->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->readFrom($path);

        self::assertSame($mockStream, $stream);
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_append_to_rethrows_exception() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new \RuntimeException('Append failed');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('appendTo')->willThrowException($exception);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Append failed');

        $fs->appendTo($path);
    }

    public function test_append_to_returns_traceable_destination_stream() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('appendTo')->with($path)->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->appendTo($path);

        self::assertInstanceOf(TraceableDestinationStream::class, $stream);
    }

    public function test_get_system_tmp_dir_delegates_to_underlying_filesystem() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $tmpPath = Path::realpath('/tmp');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('getSystemTmpDir')->willReturn($tmpPath);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        self::assertSame($tmpPath, $fs->getSystemTmpDir());
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_list_delegates_to_underlying_filesystem() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = path('file://tmp/**/*.txt');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('list')->willReturnCallback(static function () : \Generator {
            yield from [];
        });

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        \iterator_to_array($fs->list($path));

        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_mv_delegates_to_underlying_filesystem() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $from = Path::realpath('/tmp/source.txt');
        $to = Path::realpath('/tmp/dest.txt');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('mv')->willReturn(true);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $result = $fs->mv($from, $to);

        self::assertTrue($result);
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_protocol_delegates_to_underlying_filesystem() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $protocol = protocol('s3');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn($protocol);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        self::assertSame($protocol, $fs->protocol());
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_read_from_rethrows_exception() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new \RuntimeException('Read failed');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('readFrom')->willThrowException($exception);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Read failed');

        $fs->readFrom($path);
    }

    public function test_read_from_returns_traceable_source_stream() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('readFrom')->with($path)->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->readFrom($path);

        self::assertInstanceOf(TraceableSourceStream::class, $stream);
    }

    public function test_rm_delegates_to_underlying_filesystem() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('rm')->willReturn(true);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $result = $fs->rm($path);

        self::assertTrue($result);
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_status_delegates_to_underlying_filesystem() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $fileStatus = new FileStatus($path, true);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('status')->willReturn($fileStatus);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $result = $fs->status($path);

        self::assertSame($fileStatus, $result);
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_stream_tracing_enabled_wraps_streams() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor, filesystem_telemetry_options(
            traceStreams: true,
        ));
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('readFrom')->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->readFrom($path);

        self::assertInstanceOf(TraceableSourceStream::class, $stream);
    }

    public function test_write_to_rethrows_exception() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new \RuntimeException('Write failed');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('writeTo')->willThrowException($exception);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Write failed');

        $fs->writeTo($path);
    }

    public function test_write_to_returns_traceable_destination_stream() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('writeTo')->with($path)->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->writeTo($path);

        self::assertInstanceOf(TraceableDestinationStream::class, $stream);
    }
}
