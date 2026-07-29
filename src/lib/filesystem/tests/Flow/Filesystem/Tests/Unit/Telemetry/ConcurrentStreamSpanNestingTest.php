<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Telemetry\TraceableDestinationStream;
use Flow\Filesystem\Telemetry\TraceableSourceStream;
use Flow\Filesystem\Tests\Mother\FilesystemTelemetryConfigMother;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\memory_span_processor;
use function Flow\Telemetry\DSL\void_exporter;

/**
 * Streams outlive their lexical scope, so several are open at once - one bucket writer per hash bucket, one
 * partition writer per partition, one cursor per merged run. If a stream span became the active span, each
 * later stream would be parented to whichever one happened to still be open, producing a staircase as deep as
 * the number of concurrent streams instead of a flat list of siblings.
 */
final class ConcurrentStreamSpanNestingTest extends TestCase
{
    public function test_concurrently_open_destination_streams_produce_sibling_spans(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);

        $streams = [];

        for ($i = 0; $i < 5; $i++) {
            $inner = $this->createStub(DestinationStream::class);
            $inner->method('path')->willReturn(Path::realpath("/tmp/bucket-{$i}.floe"));

            $streams[] = new TraceableDestinationStream($inner, $config);
        }

        foreach ($streams as $stream) {
            $stream->close();
        }

        $spanIds = [];

        foreach ($spanProcessor->endedSpans() as $span) {
            $spanIds[(string) $span->context()->spanId] = true;
        }

        static::assertCount(5, $spanIds);

        foreach ($spanProcessor->endedSpans() as $span) {
            $parentSpanId = $span->context()->parentSpanId;

            static::assertArrayNotHasKey(
                $parentSpanId === null ? '' : (string) $parentSpanId,
                $spanIds,
                'A filesystem span must never be the parent of another filesystem span.',
            );
        }
    }

    public function test_concurrently_open_source_streams_produce_sibling_spans(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);

        $streams = [];

        for ($i = 0; $i < 5; $i++) {
            $inner = $this->createStub(SourceStream::class);
            $inner->method('path')->willReturn(Path::realpath("/tmp/run-{$i}.floe"));

            $streams[] = new TraceableSourceStream($inner, $config);
        }

        foreach ($streams as $stream) {
            $stream->close();
        }

        $spanIds = [];

        foreach ($spanProcessor->endedSpans() as $span) {
            $spanIds[(string) $span->context()->spanId] = true;
        }

        static::assertCount(5, $spanIds);

        foreach ($spanProcessor->endedSpans() as $span) {
            $parentSpanId = $span->context()->parentSpanId;

            static::assertArrayNotHasKey(
                $parentSpanId === null ? '' : (string) $parentSpanId,
                $spanIds,
                'A filesystem span must never be the parent of another filesystem span.',
            );
        }
    }

    public function test_opening_a_stream_does_not_change_the_active_span(): void
    {
        $spanProcessor = memory_span_processor(void_exporter());
        $config = FilesystemTelemetryConfigMother::create($spanProcessor);

        $tracer = $config->telemetry->tracer('test');
        $outer = $tracer->span('outer');
        $tracer->activate($outer);

        $inner = $this->createStub(DestinationStream::class);
        $inner->method('path')->willReturn(Path::realpath('/tmp/bucket.floe'));

        $stream = new TraceableDestinationStream($inner, $config);

        $activeSpan = $tracer->activeSpan();
        static::assertNotNull($activeSpan);
        static::assertTrue($activeSpan->spanId->equals($outer->context()->spanId));

        $stream->close();
    }
}
