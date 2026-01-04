<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Processor;

use Flow\Telemetry\Context\{SpanId, TraceId};
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\Processor\BatchingSpanProcessor;
use Flow\Telemetry\Tracer\{Span, SpanContext, SpanExporter, SpanKind};
use PHPUnit\Framework\TestCase;

final class BatchingSpanProcessorTest extends TestCase
{
    public function test_exporter_returns_exporter() : void
    {
        $exporter = $this->createMock(SpanExporter::class);

        $processor = new BatchingSpanProcessor($exporter, 10);

        self::assertSame($exporter, $processor->exporter());
    }

    public function test_exports_on_batch_size_reached() : void
    {
        $exporter = $this->createMock(SpanExporter::class);
        $exporter->expects(self::once())
            ->method('export')
            ->with(self::callback(fn (array $spans) => \count($spans) === 2))
            ->willReturn(true);

        $processor = new BatchingSpanProcessor($exporter, 2);

        $processor->onEnd($this->createSpan());
        $processor->onEnd($this->createSpan());
    }

    public function test_exports_remaining_on_flush() : void
    {
        $exporter = $this->createMock(SpanExporter::class);
        $exporter->expects(self::once())
            ->method('export')
            ->with(self::callback(fn (array $spans) => \count($spans) === 1))
            ->willReturn(true);

        $processor = new BatchingSpanProcessor($exporter, 10);
        $processor->onEnd($this->createSpan());

        $result = $processor->flush();

        self::assertTrue($result);
    }

    public function test_flush_returns_true_when_buffer_empty() : void
    {
        $exporter = $this->createMock(SpanExporter::class);
        $exporter->expects(self::never())
            ->method('export');

        $processor = new BatchingSpanProcessor($exporter, 10);

        $result = $processor->flush();

        self::assertTrue($result);
    }

    public function test_on_start_does_nothing() : void
    {
        $exporter = $this->createMock(SpanExporter::class);
        $exporter->expects(self::never())
            ->method('export');

        $processor = new BatchingSpanProcessor($exporter, 1);
        $processor->onStart($this->createSpan());
    }

    private function createSpan() : Span
    {
        return new Span(
            'test-span',
            SpanContext::create(TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new \DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0')
        );
    }
}
