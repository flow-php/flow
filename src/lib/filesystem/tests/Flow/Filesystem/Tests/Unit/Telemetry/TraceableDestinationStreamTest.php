<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use function Flow\Filesystem\DSL\{filesystem_telemetry_config, filesystem_telemetry_options};
use function Flow\Telemetry\DSL\{logger_provider, memory_context_storage, memory_log_processor, memory_metric_processor, memory_span_processor, meter_provider, resource, telemetry, tracer_provider, void_log_exporter, void_metric_exporter, void_span_exporter};
use Flow\Filesystem\{DestinationStream, Path};
use Flow\Filesystem\Telemetry\{FilesystemTelemetryAttributes, FilesystemTelemetryConfig, FilesystemTelemetryOptions, TraceableDestinationStream};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use PHPUnit\Framework\TestCase;

final class TraceableDestinationStreamTest extends TestCase
{
    public function test_append_tracks_bytes_written() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $data = 'Hello, World!';

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('append')->willReturnSelf();

        $stream = new TraceableDestinationStream($mockStream, $config);
        $result = $stream->append($data);

        self::assertSame($stream, $result);

        $stream->close();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('flow.filesystem.stream.write', $spans[0]->name());
        self::assertSame('destination', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
        self::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
        self::assertSame(\strlen($data), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_close_completes_lifecycle_span_with_final_attributes() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $data = 'Hello, World!';

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('append')->willReturnSelf();

        $stream = new TraceableDestinationStream($mockStream, $config);
        $stream->append($data);
        $stream->close();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('flow.filesystem.stream.write', $spans[0]->name());
        self::assertSame('destination', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
        self::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
        self::assertSame(\strlen($data), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());
    }

    public function test_close_records_exception_and_rethrows() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new \RuntimeException('Close failed');

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('append')->willReturnSelf();
        $mockStream->method('close')->willThrowException($exception);

        $stream = new TraceableDestinationStream($mockStream, $config);
        $stream->append('data');

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

    public function test_close_without_operations_still_creates_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $stream = new TraceableDestinationStream($mockStream, $config);
        $stream->close();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('flow.filesystem.stream.write', $spans[0]->name());
        self::assertSame(0, $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN]);
    }

    public function test_from_resource_tracks_bytes_written() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $resource = \fopen('php://memory', 'rb');
        self::assertIsResource($resource);

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('fromResource')->willReturnSelf();

        $stream = new TraceableDestinationStream($mockStream, $config);
        $result = $stream->fromResource($resource);

        self::assertSame($stream, $result);

        $stream->close();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('flow.filesystem.stream.write', $spans[0]->name());
        self::assertSame('destination', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
        self::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
        self::assertNotNull($spans[0]->status());
        self::assertTrue($spans[0]->status()->isOk());

        \fclose($resource);
    }

    public function test_is_open_delegates_without_affecting_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('isOpen')->willReturn(true);

        $stream = new TraceableDestinationStream($mockStream, $config);

        self::assertTrue($stream->isOpen());
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_multiple_appends_create_single_span_with_cumulative_bytes() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('append')->willReturnSelf();

        $stream = new TraceableDestinationStream($mockStream, $config);
        $stream->append('Hello');
        $stream->append(', ');
        $stream->append('World!');
        $stream->close();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('flow.filesystem.stream.write', $spans[0]->name());
        self::assertSame(13, $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN]);
    }

    public function test_path_delegates_without_affecting_span() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $stream = new TraceableDestinationStream($mockStream, $config);

        self::assertSame($path, $stream->path());
        self::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_span_created_in_constructor() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $stream = new TraceableDestinationStream($mockStream, $config);

        self::assertEmpty($spanProcessor->endedSpans());

        $stream->close();

        $spans = $spanProcessor->endedSpans();
        self::assertCount(1, $spans);
        self::assertSame('flow.filesystem.stream.write', $spans[0]->name());
        self::assertSame('destination', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
        self::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
    }

    public function test_tracing_disabled_does_not_create_spans() : void
    {
        $spanProcessor = memory_span_processor(void_span_exporter());
        $config = $this->createConfig($spanProcessor, filesystem_telemetry_options(
            traceStreams: false,
            collectMetrics: false,
        ));
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createMock(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('append')->willReturnSelf();

        $stream = new TraceableDestinationStream($mockStream, $config);
        $stream->append('data');
        $stream->close();

        self::assertEmpty($spanProcessor->endedSpans());
    }

    private function createConfig(MemorySpanProcessor $spanProcessor, ?FilesystemTelemetryOptions $options = null) : FilesystemTelemetryConfig
    {
        $clock = new SystemClock();
        $contextStorage = memory_context_storage();

        $tel = telemetry(
            resource(),
            tracer_provider($spanProcessor, $clock, $contextStorage),
            meter_provider(memory_metric_processor(void_metric_exporter()), $clock),
            logger_provider(memory_log_processor(void_log_exporter()), $clock, $contextStorage),
        );

        return filesystem_telemetry_config($tel, $clock, $options ?? filesystem_telemetry_options());
    }
}
