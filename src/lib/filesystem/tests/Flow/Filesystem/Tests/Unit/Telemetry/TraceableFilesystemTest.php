<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Telemetry\TraceableDestinationStream;
use Flow\Filesystem\Telemetry\TraceableFilesystem;
use Flow\Filesystem\Telemetry\TraceableSourceStream;
use Flow\Filesystem\Tests\Mother\FilesystemTelemetryConfigMother;
use Generator;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function Flow\Filesystem\DSL\filesystem_telemetry_options;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Telemetry\DSL\memory_span_processor;
use function Flow\Telemetry\DSL\void_exporter;
use function iterator_to_array;

final class TraceableFilesystemTest extends TestCase
{
    public function test_all_telemetry_disabled_does_not_wrap_streams(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor, filesystem_telemetry_options(
            trace_streams: false,
            collect_metrics: false,
        ));
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createStub(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createStub(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem->method('readFrom')->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->readFrom($path);

        static::assertSame($mockStream, $stream);
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_append_to_rethrows_exception(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new RuntimeException('Append failed');

        $mockFilesystem = $this->createStub(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem->method('appendTo')->willThrowException($exception);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Append failed');

        $fs->appendTo($path);
    }

    public function test_append_to_returns_traceable_destination_stream(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem->expects(self::once())->method('appendTo')->with($path)->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->appendTo($path);

        static::assertInstanceOf(TraceableDestinationStream::class, $stream);
    }

    public function test_get_system_tmp_dir_delegates_to_underlying_filesystem(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $tmpPath = Path::realpath('/tmp');

        $mockFilesystem = $this->createStub(Filesystem::class);
        $mockFilesystem->method('getSystemTmpDir')->willReturn($tmpPath);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        static::assertSame($tmpPath, $fs->getSystemTmpDir());
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_list_delegates_to_underlying_filesystem(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = path('file://tmp/**/*.txt');

        $mockFilesystem = $this->createStub(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem
            ->method('list')
            ->willReturnCallback(static function (): Generator {
                yield from [];
            });

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        iterator_to_array($fs->list($path));

        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_mount_delegates_to_underlying_filesystem(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $mount = new Mount('s3');

        $mockFilesystem = $this->createStub(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn($mount);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        static::assertSame($mount, $fs->mount());
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_mv_delegates_to_underlying_filesystem(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $from = Path::realpath('/tmp/source.txt');
        $to = Path::realpath('/tmp/dest.txt');

        $mockFilesystem = $this->createStub(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem->method('mv')->willReturn(true);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $result = $fs->mv($from, $to);

        static::assertTrue($result);
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_read_from_rethrows_exception(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new RuntimeException('Read failed');

        $mockFilesystem = $this->createStub(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem->method('readFrom')->willThrowException($exception);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Read failed');

        $fs->readFrom($path);
    }

    public function test_read_from_returns_traceable_source_stream(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createStub(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem->expects(self::once())->method('readFrom')->with($path)->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->readFrom($path);

        static::assertInstanceOf(TraceableSourceStream::class, $stream);
    }

    public function test_rm_delegates_to_underlying_filesystem(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockFilesystem = $this->createStub(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem->method('rm')->willReturn(true);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $result = $fs->rm($path);

        static::assertTrue($result);
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_traceable_filesystem_delegates_supports_to_the_decorated_one(): void
    {
        $decorated = memory_filesystem();
        $fs = new TraceableFilesystem(
            $decorated,
            FilesystemTelemetryConfigMother::create(memory_span_processor(void_exporter())),
        );

        static::assertSame(
            $decorated->supports(path('memory://orders.csv')),
            $fs->supports(path('memory://orders.csv')),
        );
        static::assertSame($decorated->supports(path('/tmp/orders.csv')), $fs->supports(path('/tmp/orders.csv')));
        static::assertTrue($fs->supports(path('memory://orders.csv')));
        static::assertFalse($fs->supports(path('/tmp/orders.csv')));
    }

    public function test_status_delegates_to_underlying_filesystem(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $fileStatus = new FileStatus($path, true);

        $mockFilesystem = $this->createStub(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem->method('status')->willReturn($fileStatus);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $result = $fs->status($path);

        static::assertSame($fileStatus, $result);
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_stream_tracing_enabled_wraps_streams(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create(
            $spanProcessor,
            filesystem_telemetry_options(trace_streams: true),
        );
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createStub(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createStub(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem->method('readFrom')->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->readFrom($path);

        static::assertInstanceOf(TraceableSourceStream::class, $stream);
    }

    public function test_write_to_rethrows_exception(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new RuntimeException('Write failed');

        $mockFilesystem = $this->createStub(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem->method('writeTo')->willThrowException($exception);

        $fs = new TraceableFilesystem($mockFilesystem, $config);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Write failed');

        $fs->writeTo($path);
    }

    public function test_write_to_returns_traceable_destination_stream(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $mockFilesystem = $this->createMock(Filesystem::class);
        $mockFilesystem->method('mount')->willReturn(new Mount('file'));
        $mockFilesystem->expects(self::once())->method('writeTo')->with($path)->willReturn($mockStream);

        $fs = new TraceableFilesystem($mockFilesystem, $config);
        $stream = $fs->writeTo($path);

        static::assertInstanceOf(TraceableDestinationStream::class, $stream);
    }
}
