<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Path;
use Flow\Filesystem\Telemetry\FilesystemTelemetryAttributes;
use Flow\Filesystem\Telemetry\TraceableDestinationStream;
use Flow\Filesystem\Tests\Mother\FilesystemTelemetryConfigMother;
use Flow\Telemetry\SemConvAttributes;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function fclose;
use function Flow\Filesystem\DSL\filesystem_telemetry_options;
use function Flow\Telemetry\DSL\memory_span_processor;
use function Flow\Telemetry\DSL\void_exporter;
use function fopen;
use function strlen;

final class TraceableDestinationStreamTest extends TestCase
{
    public function test_append_tracks_bytes_written(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $data = 'Hello, World!';

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('append')->willReturnSelf();

        $stream = new TraceableDestinationStream($mockStream, $config);
        $result = $stream->append($data);

        static::assertSame($stream, $result);

        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('filesystem.write', $spans[0]->name());
        static::assertSame('destination', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
        static::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
        static::assertSame(
            strlen($data),
            $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN],
        );
        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($spans[0]->status());
    }

    public function test_close_completes_lifecycle_span_with_final_attributes(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $data = 'Hello, World!';

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('append')->willReturnSelf();

        $stream = new TraceableDestinationStream($mockStream, $config);
        $stream->append($data);
        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('filesystem.write', $spans[0]->name());
        static::assertSame('destination', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
        static::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
        static::assertSame(
            strlen($data),
            $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN],
        );
        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($spans[0]->status());
    }

    public function test_close_records_exception_and_rethrows(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $exception = new RuntimeException('Close failed');

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('append')->willReturnSelf();
        $mockStream->method('close')->willThrowException($exception);

        $stream = new TraceableDestinationStream($mockStream, $config);
        $stream->append('data');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Close failed');

        try {
            $stream->close();
        } finally {
            $spans = $spanProcessor->endedSpans();
            static::assertCount(1, $spans);
            $status = $spans[0]->status();
            static::assertNotNull($status);
            static::assertTrue($status->isError());
            static::assertSame(RuntimeException::class, $spans[0]->attributes()[SemConvAttributes::ERROR_TYPE]);
            static::assertNotEmpty($spans[0]->events());
        }
    }

    public function test_close_without_operations_still_creates_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $stream = new TraceableDestinationStream($mockStream, $config);
        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('filesystem.write', $spans[0]->name());
        static::assertSame(0, $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN]);
    }

    public function test_from_resource_tracks_bytes_written(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');
        $resource = fopen('php://memory', 'rb');
        static::assertIsResource($resource);

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('fromResource')->willReturnSelf();

        $stream = new TraceableDestinationStream($mockStream, $config);
        $result = $stream->fromResource($resource);

        static::assertSame($stream, $result);

        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('filesystem.write', $spans[0]->name());
        static::assertSame('destination', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
        static::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
        // OTEL spec: instrumentation leaves the status Unset on success.
        static::assertNull($spans[0]->status());

        fclose($resource);
    }

    public function test_is_open_delegates_without_affecting_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('isOpen')->willReturn(true);

        $stream = new TraceableDestinationStream($mockStream, $config);

        static::assertTrue($stream->isOpen());
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_multiple_appends_create_single_span_with_cumulative_bytes(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('append')->willReturnSelf();

        $stream = new TraceableDestinationStream($mockStream, $config);
        $stream->append('Hello');
        $stream->append(', ');
        $stream->append('World!');
        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('filesystem.write', $spans[0]->name());
        static::assertSame(13, $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_BYTES_TOTAL_WRITTEN]);
    }

    public function test_path_delegates_without_affecting_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $stream = new TraceableDestinationStream($mockStream, $config);

        static::assertSame($path, $stream->path());
        static::assertEmpty($spanProcessor->endedSpans());
    }

    public function test_span_created_in_constructor(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);

        $stream = new TraceableDestinationStream($mockStream, $config);

        static::assertEmpty($spanProcessor->endedSpans());

        $stream->close();

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('filesystem.write', $spans[0]->name());
        static::assertSame('destination', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_STREAM_TYPE]);
        static::assertSame($path->uri(), $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_PATH_URI]);
        static::assertSame('file', $spans[0]->attributes()[FilesystemTelemetryAttributes::ATTR_FILESYSTEM_PROTOCOL]);
    }

    public function test_tracing_disabled_does_not_create_spans(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor, filesystem_telemetry_options(
            trace_streams: false,
            collect_metrics: false,
        ));
        $path = Path::realpath('/tmp/test.txt');

        $mockStream = $this->createStub(DestinationStream::class);
        $mockStream->method('path')->willReturn($path);
        $mockStream->method('append')->willReturnSelf();

        $stream = new TraceableDestinationStream($mockStream, $config);
        $stream->append('data');
        $stream->close();

        static::assertEmpty($spanProcessor->endedSpans());
    }
}
