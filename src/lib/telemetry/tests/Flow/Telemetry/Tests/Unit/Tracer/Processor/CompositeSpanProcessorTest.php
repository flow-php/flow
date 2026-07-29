<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer\Processor;

use DateTimeImmutable;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\Processor\CompositeSpanProcessor;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanProcessor;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class CompositeSpanProcessorTest extends TestCase
{
    public function test_flush_continues_after_child_throws_and_routes_to_error_handler(): void
    {
        $throwing = $this->createStub(SpanProcessor::class);
        $throwing->method('flush')->willThrowException(new RuntimeException('flush blew up'));

        $sibling = $this->createMock(SpanProcessor::class);
        $sibling->expects(self::once())->method('flush')->willReturn(true);

        $spy = new ErrorHandlerSpy();
        $composite = new CompositeSpanProcessor([$throwing, $sibling], $spy);

        static::assertFalse($composite->flush());
        static::assertSame(1, $spy->count());
    }

    public function test_flush_returns_false_when_any_fails(): void
    {
        $processor1 = $this->createMock(SpanProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(SpanProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(false);

        $composite = new CompositeSpanProcessor([$processor1, $processor2]);

        static::assertFalse($composite->flush());
    }

    public function test_flush_returns_true_when_all_succeed(): void
    {
        $processor1 = $this->createMock(SpanProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(SpanProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(true);

        $composite = new CompositeSpanProcessor([$processor1, $processor2]);

        static::assertTrue($composite->flush());
    }

    public function test_forwards_on_end_to_all_processors(): void
    {
        $processor1 = $this->createMock(SpanProcessor::class);
        $processor1->expects(self::once())->method('onEnd');

        $processor2 = $this->createMock(SpanProcessor::class);
        $processor2->expects(self::once())->method('onEnd');

        $composite = new CompositeSpanProcessor([$processor1, $processor2]);
        $composite->onEnd($this->createSpan());
    }

    public function test_forwards_on_start_to_all_processors(): void
    {
        $processor1 = $this->createMock(SpanProcessor::class);
        $processor1->expects(self::once())->method('onStart');

        $processor2 = $this->createMock(SpanProcessor::class);
        $processor2->expects(self::once())->method('onStart');

        $composite = new CompositeSpanProcessor([$processor1, $processor2]);
        $composite->onStart($this->createSpan());
    }

    public function test_on_end_continues_after_child_throws_and_routes_to_error_handler(): void
    {
        $throwing = $this->createStub(SpanProcessor::class);
        $throwing->method('onEnd')->willThrowException(new RuntimeException('end blew up'));

        $sibling = $this->createMock(SpanProcessor::class);
        $sibling->expects(self::once())->method('onEnd');

        $spy = new ErrorHandlerSpy();
        $composite = new CompositeSpanProcessor([$throwing, $sibling], $spy);
        $composite->onEnd($this->createSpan());

        static::assertSame(1, $spy->count());
    }

    public function test_on_start_continues_after_child_throws_and_routes_to_error_handler(): void
    {
        $throwing = $this->createStub(SpanProcessor::class);
        $throwing->method('onStart')->willThrowException(new RuntimeException('start blew up'));

        $sibling = $this->createMock(SpanProcessor::class);
        $sibling->expects(self::once())->method('onStart');

        $spy = new ErrorHandlerSpy();
        $composite = new CompositeSpanProcessor([$throwing, $sibling], $spy);
        $composite->onStart($this->createSpan());

        static::assertSame(1, $spy->count());
    }

    public function test_works_with_empty_processors_array(): void
    {
        $composite = new CompositeSpanProcessor([]);

        $composite->onStart($this->createSpan());
        $composite->onEnd($this->createSpan());

        static::assertTrue($composite->flush());
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
