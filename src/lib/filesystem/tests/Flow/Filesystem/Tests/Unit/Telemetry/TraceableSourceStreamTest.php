<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Telemetry\FilesystemTelemetryAttributes;
use Flow\Filesystem\Telemetry\TraceableSourceStream;
use Flow\Filesystem\Tests\Mother\FilesystemTelemetryConfigMother;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\filesystem_telemetry_options;
use function Flow\Telemetry\DSL\memory_span_processor;
use function Flow\Telemetry\DSL\void_exporter;

final class TraceableSourceStreamTest extends TestCase
{
    public function test_close_completes_lifecycle_span_with_final_attributes(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $content = 'Hello, World!';

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('content')->willReturn($content);

        $stream = new TraceableSourceStream($mockStream, $config);
        $stream->content();
        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('Read test.txt', $spans[0]->name());
        static::assertSame('source', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
        static::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
        static::assertSame(
            \strlen($content),
            $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ],
        );
        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isOk());
    }

    public function test_close_records_exception_and_rethrows(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new \RuntimeException('Close failed');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('content')->willReturn('data');
        $mockStream->method('close')->willThrowException($exception);

        $stream = new TraceableSourceStream($mockStream, $config);
        $stream->content();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Close failed');

        try {
            $stream->close();
        } finally {
            $spans = $spanProcessor->endedSpans();
            static::assertCount(1, $spans);
            $status = $spans[0]->status();
            static::assertNotNull($status);
            static::assertTrue($status->isError());
            static::assertNotEmpty($spans[0]->events());
        }
    }

    public function test_close_without_operations_still_creates_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $stream = new TraceableSourceStream($mockStream, $config);
        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('Read test.txt', $spans[0]->name());
        static::assertSame(0, $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ]);
    }

    public function test_content_tracks_bytes_read(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $content = 'Hello, World!';

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('content')->willReturn($content);

        $stream = new TraceableSourceStream($mockStream, $config);
        $result = $stream->content();

        static::assertSame($content, $result);

        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('Read test.txt', $spans[0]->name());
        static::assertSame(
            \strlen($content),
            $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ],
        );
    }

    public function test_is_open_delegates_without_affecting_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('isOpen')->willReturn(true);

        $stream = new TraceableSourceStream($mockStream, $config);

        static::assertTrue($stream->isOpen());
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_iterate_tracks_bytes_read_cumulatively(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $chunks = ['Hello', ', ', 'World', '!'];

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream
            ->method('iterate')
            ->willReturnCallback(static function () use ($chunks): \Generator {
                yield from $chunks;
            });

        $stream = new TraceableSourceStream($mockStream, $config);
        $result = \iterator_to_array($stream->iterate());

        static::assertSame($chunks, $result);

        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('Read test.txt', $spans[0]->name());
        static::assertSame(
            \strlen(\implode('', $chunks)),
            $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ],
        );
    }

    public function test_multiple_operations_track_cumulative_bytes(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('read')->willReturnOnConsecutiveCalls('Hello', 'World', '!');

        $stream = new TraceableSourceStream($mockStream, $config);
        $stream->read(5, 0);
        $stream->read(5, 5);
        $stream->read(1, 10);
        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame(11, $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ]);
    }

    public function test_path_delegates_without_affecting_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $stream = new TraceableSourceStream($mockStream, $config);

        static::assertSame($path, $stream->path());
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_read_lines_tracks_bytes_read(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $lines = ['line1', 'line2', 'line3'];

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream
            ->method('readLines')
            ->willReturnCallback(static function () use ($lines): \Generator {
                yield from $lines;
            });

        $stream = new TraceableSourceStream($mockStream, $config);
        $result = \iterator_to_array($stream->readLines());

        static::assertSame($lines, $result);

        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('Read test.txt', $spans[0]->name());
    }

    public function test_read_tracks_bytes_read(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $content = 'Hello';

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('read')->with(10, 0)->willReturn($content);

        $stream = new TraceableSourceStream($mockStream, $config);
        $result = $stream->read(10, 0);

        static::assertSame($content, $result);

        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('Read test.txt', $spans[0]->name());
        static::assertSame(
            \strlen($content),
            $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_READ],
        );
    }

    public function test_size_delegates_without_affecting_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('size')->willReturn(1024);

        $stream = new TraceableSourceStream($mockStream, $config);

        static::assertSame(1024, $stream->size());
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_span_created_in_constructor(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $stream = new TraceableSourceStream($mockStream, $config);

        static::assertEmpty($spanProcessor->endedSpans());

        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('Read test.txt', $spans[0]->name());
        static::assertSame('source', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
        static::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
        static::assertSame('file', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL]);
    }

    public function test_tracing_disabled_does_not_create_spans(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor, filesystem_telemetry_options(
            traceStreams: false,
            collectMetrics: false,
        ));
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('content')->willReturn('Hello');

        $stream = new TraceableSourceStream($mockStream, $config);
        $result = $stream->content();
        $stream->close();

        static::assertSame('Hello', $result);
        static::assertEmpty($spanProcessor->endedSpans());
    }
}
