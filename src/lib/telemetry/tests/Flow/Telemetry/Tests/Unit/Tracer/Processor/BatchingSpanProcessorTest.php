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
use Flow\Telemetry\Tests\Mother\ExporterSpy;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tests\Mother\SpanMother;
use Flow\Telemetry\Tracer\Processor\BatchingSpanProcessor;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class BatchingSpanProcessorTest extends TestCase
{
    public function test_exports_on_batch_size_reached(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter
            ->expects(self::once())
            ->method('export')
            ->with(static::callback(
                static fn(mixed $signal) => (
                    $signal instanceof Signals
                    && $signal->type === SignalType::TRACES
                    && $signal->count() === 2
                ),
            ))
            ->willReturn(true);

        $processor = new BatchingSpanProcessor($exporter, 2);

        $processor->onEnd($this->createSpan());
        $processor->onEnd($this->createSpan());
    }

    public function test_exports_remaining_on_flush(): void
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

        $processor = new BatchingSpanProcessor($exporter, 10);
        $processor->onEnd($this->createSpan());

        $result = $processor->flush();

        static::assertTrue($result);
    }

    public function test_flush_returns_true_when_buffer_empty(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->expects(self::never())->method('export');

        $processor = new BatchingSpanProcessor($exporter, 10);

        $result = $processor->flush();

        static::assertTrue($result);
    }

    public function test_flush_routes_exporter_throwable_to_error_handler(): void
    {
        $exporter = $this->createStub(Exporter::class);
        $exporter->method('export')->willThrowException(new RuntimeException('exporter exploded'));
        $spy = new ErrorHandlerSpy();

        $processor = new BatchingSpanProcessor($exporter, 10, $spy);
        $processor->onEnd($this->createSpan());

        static::assertFalse($processor->flush());
        static::assertSame(1, $spy->count());
        static::assertSame('exporter exploded', $spy->last()?->getMessage());
    }

    public function test_on_start_does_nothing(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->expects(self::never())->method('export');

        $processor = new BatchingSpanProcessor($exporter, 1);
        $processor->onStart($this->createSpan());
    }

    public function test_age_disabled_by_default_does_not_export_until_flush(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingSpanProcessor($exporter, 512, new ErrorHandlerSpy(), null);

        $processor->onEnd(SpanMother::create());
        $processor->onEnd(SpanMother::create());

        static::assertSame(0, $exporter->exportedCount());

        static::assertTrue($processor->flush());
        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_age_set_but_not_due_does_not_export_until_flush(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingSpanProcessor($exporter, 512, new ErrorHandlerSpy(), 3600.0);

        $processor->onEnd(SpanMother::create());
        $processor->onEnd(SpanMother::create());

        static::assertSame(0, $exporter->exportedCount());

        static::assertTrue($processor->flush());
        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_age_flush_when_deadline_exceeded_on_end(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingSpanProcessor($exporter, 512, new ErrorHandlerSpy(), 0.01);

        $processor->onEnd(SpanMother::create());
        usleep(20_000);
        $processor->onEnd(SpanMother::create());

        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_size_trigger_still_flushes_when_age_set_but_not_due(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingSpanProcessor($exporter, 2, new ErrorHandlerSpy(), 3600.0);

        $processor->onEnd(SpanMother::create());
        $processor->onEnd(SpanMother::create());

        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_age_clock_resets_after_flush(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingSpanProcessor($exporter, 512, new ErrorHandlerSpy(), 0.01);

        $processor->onEnd(SpanMother::create());
        usleep(20_000);
        static::assertTrue($processor->flush());
        static::assertSame(1, $exporter->exportedCount());

        $processor->onEnd(SpanMother::create());

        static::assertSame(1, $exporter->exportedCount());
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
