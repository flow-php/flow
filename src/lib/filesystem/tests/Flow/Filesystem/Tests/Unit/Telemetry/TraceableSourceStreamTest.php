<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use function Flow\Filesystem\DSL\{filesystem_telemetry_config, filesystem_telemetry_options};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\Filesystem\{Path, SourceStream};
use Flow\Filesystem\Telemetry\{FilesystemTelemetryAttributes, FilesystemTelemetryConfig, TraceableSourceStream};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use PHPUnit\Framework\TestCase;

final class TraceableSourceStreamTest extends TestCase
{
    public function test_close_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $stream = new TraceableSourceStream($mockStream, $config);
        $stream->close();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('SourceStream::close', $spans[0]->name());
        self::assertSame('source', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
        self::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_close_records_exception_and_rethrows() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new \RuntimeException('Close failed');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('close')->willThrowException($exception);

        $stream = new TraceableSourceStream($mockStream, $config);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Close failed');

        try {
            $stream->close();
        } finally {
            $spans = $spanProcessor->endedSpans();
            self::assertCount(1, $spans);
            self::assertNotNull($spans[0]->status());
            self::assertTrue($spans[0]->status()->isError());
            self::assertNotEmpty($spans[0]->events());
        }
    }

    public function test_content_creates_span_with_bytes_read() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $content = 'Hello, World!';

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('content')->willReturn($content);

        $stream = new TraceableSourceStream($mockStream, $config);
        $result = $stream->content();

        self::assertSame($content, $result);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('SourceStream::content', $spans[0]->name());
        self::assertSame(\strlen($content), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_READ]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_content_records_exception_and_rethrows() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new \RuntimeException('Content read failed');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('content')->willThrowException($exception);

        $stream = new TraceableSourceStream($mockStream, $config);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Content read failed');

        try {
            $stream->content();
        } finally {
            $spans = $spanProcessor->endedSpans();
            self::assertCount(1, $spans);
            self::assertNotNull($spans[0]->status());
            self::assertTrue($spans[0]->status()->isError());
        }
    }

    public function test_is_open_delegates_without_creating_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('isOpen')->willReturn(true);

        $stream = new TraceableSourceStream($mockStream, $config);

        self::assertTrue($stream->isOpen());
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_iterate_creates_span_and_tracks_bytes_read() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $chunks = ['Hello', ', ', 'World', '!'];

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('iterate')->willReturnCallback(static function () use ($chunks) : \Generator {
            yield from $chunks;
        });

        $stream = new TraceableSourceStream($mockStream, $config);
        $result = \iterator_to_array($stream->iterate());

        self::assertSame($chunks, $result);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('SourceStream::iterate', $spans[0]->name());
        self::assertSame(\strlen(\implode('', $chunks)), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_READ]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_iterate_records_bytes_read_on_exception() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('iterate')->willReturnCallback(static function () : \Generator {
            yield 'first';
            yield 'second';

            throw new \RuntimeException('Iterate failed');
        });

        $stream = new TraceableSourceStream($mockStream, $config);

        $this->expectException(\RuntimeException::class);

        try {
            \iterator_to_array($stream->iterate());
        } finally {
            $spans = $spanProcessor->endedSpans();
            self::assertCount(1, $spans);
            self::assertNotNull($spans[0]->status());
            self::assertTrue($spans[0]->status()->isError());
            self::assertSame(\strlen('firstsecond'), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_READ]);
        }
    }

    public function test_path_delegates_without_creating_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);

        $stream = new TraceableSourceStream($mockStream, $config);

        self::assertSame($path, $stream->path());
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_read_creates_span_with_bytes_read() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $content = 'Hello';

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('read')->with(10, 0)->willReturn($content);

        $stream = new TraceableSourceStream($mockStream, $config);
        $result = $stream->read(10, 0);

        self::assertSame($content, $result);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('SourceStream::read', $spans[0]->name());
        self::assertSame(\strlen($content), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_READ]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_read_lines_creates_span_and_tracks_bytes_read() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $lines = ['line1', 'line2', 'line3'];

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('readLines')->willReturnCallback(static function () use ($lines) : \Generator {
            yield from $lines;
        });

        $stream = new TraceableSourceStream($mockStream, $config);
        $result = \iterator_to_array($stream->readLines());

        self::assertSame($lines, $result);

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('SourceStream::readLines', $spans[0]->name());
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_size_delegates_without_creating_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('size')->willReturn(1024);

        $stream = new TraceableSourceStream($mockStream, $config);

        self::assertSame(1024, $stream->size());
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_tracing_disabled_does_not_create_spans() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, filesystem_telemetry_options(
            traceFilesystemOperations: true,
            traceStreamOperations: false,
        ));
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(SourceStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('content')->willReturn('Hello');

        $stream = new TraceableSourceStream($mockStream, $config);
        $result = $stream->content();

        self::assertSame('Hello', $result);
        self::assertEmpty($spanProcessor->endedSpans());
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
