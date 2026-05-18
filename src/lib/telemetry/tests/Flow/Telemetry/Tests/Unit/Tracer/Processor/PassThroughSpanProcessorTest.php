<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Processor;

use DateTimeImmutable;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Signal\SignalType;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\Processor\PassThroughSpanProcessor;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class PassThroughSpanProcessorTest extends TestCase
{
    public function test_exports_each_span_individually(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter
            ->expects(self::exactly(3))
            ->method('export')
            ->with(static::callback(
                static fn(mixed $signal) => (
                    $signal instanceof Signals
                    && $signal->type === SignalType::TRACES
                    && $signal->count() === 1
                ),
            ))
            ->willReturn(true);

        $processor = new PassThroughSpanProcessor($exporter);
        $processor->onEnd($this->createSpan());
        $processor->onEnd($this->createSpan());
        $processor->onEnd($this->createSpan());
    }

    public function test_exports_span_immediately_on_end(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter
            ->expects(self::once())
            ->method('export')
            ->with(static::callback(
                static fn(mixed $signal) => (
                    $signal instanceof Signals
                    && $signal->type === SignalType::TRACES
                    && $signal->count() === 1
                ),
            ))
            ->willReturn(true);

        $processor = new PassThroughSpanProcessor($exporter);
        $processor->onEnd($this->createSpan());
    }

    public function test_flush_returns_true(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $processor = new PassThroughSpanProcessor($exporter);

        $result = $processor->flush();

        static::assertTrue($result);
    }

    public function test_on_end_routes_exporter_throwable_to_error_handler(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->method('export')->willThrowException(new RuntimeException('exporter exploded'));
        $spy = new ErrorHandlerSpy();

        $processor = new PassThroughSpanProcessor($exporter, $spy);
        $processor->onEnd($this->createSpan());

        static::assertSame(1, $spy->count());
        static::assertSame('exporter exploded', $spy->last()?->getMessage());
    }

    public function test_on_start_does_nothing(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->expects(self::never())->method('export');

        $processor = new PassThroughSpanProcessor($exporter);
        $processor->onStart($this->createSpan());
    }

    private function createSpan(): Span
    {
        return new Span(
            'test-span',
            SpanContext::create(TraceId::generate(), SpanId::generate()),
            SpanKind::INTERNAL,
            new DateTimeImmutable(),
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
        );
    }
}
