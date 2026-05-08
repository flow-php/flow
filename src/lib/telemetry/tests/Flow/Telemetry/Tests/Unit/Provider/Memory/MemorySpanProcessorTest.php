<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Memory;

use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Provider\Memory\{MemoryExporter, MemorySpanProcessor};
use Flow\Telemetry\Tests\Mother\{ErrorHandlerSpy, SpanMother};
use Flow\Telemetry\Tracer\SpanProcessor;
use PHPUnit\Framework\TestCase;

final class MemorySpanProcessorTest extends TestCase
{
    public function test_ended_spans_for_trace_returns_empty_for_unknown_trace() : void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());

        self::assertSame([], $processor->endedSpansForTrace('unknown-trace-id'));
    }

    public function test_ended_spans_returns_all_ended_spans() : void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $span1 = SpanMother::withName('span-1');
        $span2 = SpanMother::withName('span-2');

        $processor->onEnd($span1);
        $processor->onEnd($span2);

        $endedSpans = $processor->endedSpans();
        self::assertCount(2, $endedSpans);
        self::assertContains($span1, $endedSpans);
        self::assertContains($span2, $endedSpans);
    }

    public function test_flush_exports_ended_spans() : void
    {
        $exporter = new MemoryExporter();
        $processor = new MemorySpanProcessor($exporter);
        $span = SpanMother::withName('test-span');

        $processor->onEnd($span);
        $result = $processor->flush();

        self::assertTrue($result);
        self::assertCount(1, $exporter->spans());
        self::assertSame($span, $exporter->spans()[0]);
    }

    public function test_flush_returns_true_when_no_spans() : void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());

        self::assertTrue($processor->flush());
    }

    public function test_flush_routes_exporter_throwable_to_error_handler() : void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->method('export')->willThrowException(new \RuntimeException('exporter exploded'));
        $spy = new ErrorHandlerSpy();

        $processor = new MemorySpanProcessor($exporter, $spy);
        $processor->onEnd(SpanMother::withName('span'));

        self::assertFalse($processor->flush());
        self::assertSame(1, $spy->count());
        self::assertSame('exporter exploded', $spy->last()?->getMessage());
    }

    public function test_implements_span_processor() : void
    {
        self::assertInstanceOf(SpanProcessor::class, new MemorySpanProcessor(new MemoryExporter()));
    }

    public function test_on_end_stores_span_by_trace_id() : void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $traceId = TraceId::generate();
        $span = SpanMother::withTraceId($traceId);

        $processor->onEnd($span);

        $spansForTrace = $processor->endedSpansForTrace($traceId->toHex());
        self::assertCount(1, $spansForTrace);
        self::assertSame($span, $spansForTrace[0]);
    }

    public function test_on_start_stores_span_by_trace_id() : void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $traceId = TraceId::generate();
        $span = SpanMother::withTraceId($traceId);

        $processor->onStart($span);

        $spansForTrace = $processor->startedSpansForTrace($traceId->toHex());
        self::assertCount(1, $spansForTrace);
        self::assertSame($span, $spansForTrace[0]);
    }

    public function test_reset_clears_all_spans() : void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $span = SpanMother::withName('test-span');

        $processor->onStart($span);
        $processor->onEnd($span);

        self::assertCount(1, $processor->startedSpans());
        self::assertCount(1, $processor->endedSpans());

        $processor->reset();

        self::assertSame([], $processor->startedSpans());
        self::assertSame([], $processor->endedSpans());
    }

    public function test_started_spans_for_trace_returns_empty_for_unknown_trace() : void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());

        self::assertSame([], $processor->startedSpansForTrace('unknown-trace-id'));
    }

    public function test_started_spans_returns_all_started_spans() : void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $span1 = SpanMother::withName('span-1');
        $span2 = SpanMother::withName('span-2');

        $processor->onStart($span1);
        $processor->onStart($span2);

        $startedSpans = $processor->startedSpans();
        self::assertCount(2, $startedSpans);
        self::assertContains($span1, $startedSpans);
        self::assertContains($span2, $startedSpans);
    }

    public function test_trace_ids_returns_all_unique_trace_ids() : void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $traceId1 = TraceId::generate();
        $traceId2 = TraceId::generate();
        $span1 = SpanMother::withTraceId($traceId1);
        $span2 = SpanMother::withTraceId($traceId2);

        $processor->onStart($span1);
        $processor->onEnd($span2);

        $traceIds = $processor->traceIds();
        self::assertCount(2, $traceIds);
        self::assertContains($traceId1->toHex(), $traceIds);
        self::assertContains($traceId2->toHex(), $traceIds);
    }
}
