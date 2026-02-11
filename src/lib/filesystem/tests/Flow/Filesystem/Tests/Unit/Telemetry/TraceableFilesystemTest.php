<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use function Flow\Filesystem\DSL\{filesystem_telemetry_config, filesystem_telemetry_options, path, protocol};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\Filesystem\{DestinationStream, FileStatus, Filesystem, Path, SourceStream};
use Flow\Filesystem\Telemetry\{FilesystemTelemetryAttributes, FilesystemTelemetryConfig, TraceableDestinationStream, TraceableFilesystem, TraceableSourceStream};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use PHPUnit\Framework\TestCase;

final class TraceableFilesystemTest extends TestCase
{
    public function test_append_to_creates_span_and_returns_traceable_destination_stream() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('appendTo')->with($path)->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->appendTo($path);

        self::assertInstanceOf(TraceableDestinationStream::class, $stream);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('Filesystem::appendTo', $spans[0]->name());
        self::assertSame('file', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL]);
        self::assertSame('appendTo', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION]);
        self::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_append_to_records_exception_and_rethrows() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new \RuntimeException('Append failed');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('appendTo')->willThrowException($exception);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Append failed');

        try {
            $fs->appendTo($path);
        } finally {
            $spans = $spanProcessor->endedSpans();
            self::assertCount(1, $spans);
            self::assertNotNull($spans[0]->status());
            self::assertTrue($spans[0]->status()->isError());
            self::assertSame('Append failed', $spans[0]->status()->description);
            self::assertNotEmpty($spans[0]->events());
            self::assertSame('exception', $spans[0]->events()[0]->name());
        }
    }

    public function test_get_system_tmp_dir_delegates_to_underlying_filesystem() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $tmpPath = Path::realpath('/tmp');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('getSystemTmpDir')->willReturn($tmpPath);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        self::assertSame($tmpPath, $fs->getSystemTmpDir());
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_list_creates_span_with_pattern_attribute() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = path('file://tmp/**/*.txt');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('list')->willReturnCallback(static function () : \Generator {
            yield from [];
        });

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        \iterator_to_array($fs->list($path));

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('Filesystem::list', $spans[0]->name());
        self::assertTrue($spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_IS_PATTERN]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_mv_creates_span_with_from_and_to_paths() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $from = Path::realpath('/tmp/source.txt');
        $to = Path::realpath('/tmp/dest.txt');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('mv')->willReturn(true);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $result = $fs->mv($from, $to);

        self::assertTrue($result);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('Filesystem::mv', $spans[0]->name());
        self::assertSame($from->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_FROM]);
        self::assertSame($to->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_TO]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_protocol_delegates_to_underlying_filesystem() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $protocol = protocol('s3');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn($protocol);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        self::assertSame($protocol, $fs->protocol());
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_read_from_creates_span_and_returns_traceable_source_stream() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('readFrom')->with($path)->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->readFrom($path);

        self::assertInstanceOf(TraceableSourceStream::class, $stream);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('Filesystem::readFrom', $spans[0]->name());
        self::assertSame('file', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL]);
        self::assertSame('readFrom', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_rm_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('rm')->willReturn(true);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $result = $fs->rm($path);

        self::assertTrue($result);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('Filesystem::rm', $spans[0]->name());
        self::assertSame('rm', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_status_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $fileStatus = new FileStatus($path, true);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('status')->willReturn($fileStatus);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $result = $fs->status($path);

        self::assertSame($fileStatus, $result);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('Filesystem::status', $spans[0]->name());
        self::assertSame('status', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_tracing_disabled_does_not_create_spans_but_still_wraps_streams() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, filesystem_telemetry_options(
            traceFilesystemOperations: false,
            traceStreamOperations: true,
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
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_tracing_disabled_does_not_wrap_streams_when_stream_tracing_disabled() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, filesystem_telemetry_options(
            traceFilesystemOperations: false,
            traceStreamOperations: false,
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

    public function test_write_to_creates_span_and_returns_traceable_destination_stream() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('protocol')->willReturn(protocol('file'));
        $mockFilesystem->method('writeTo')->with($path)->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->writeTo($path);

        self::assertInstanceOf(TraceableDestinationStream::class, $stream);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('Filesystem::writeTo', $spans[0]->name());
        self::assertSame('writeTo', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_OPERATION]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    private function createConfig(MemorySpanProcessor $spanProcessor, ?\Flow\Filesystem\Telemetry\FilesystemTelemetryOptions $options = null) : FilesystemTelemetryConfig
    {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $tel = telemetry(
            resource(),
            tracer_provider($spanProcessor, $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_metric_exporter()), $clock),
            logger_provider(memory_log_processor(void_log_exporter()), $clock, $contextStorage),
        );

        return filesystem_telemetry_config($tel, $options ?? filesystem_telemetry_options());
    }
}
