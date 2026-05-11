<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\SpanProcessor;
use Flow\Telemetry\Tracer\Tracer;
use PHPUnit\Framework\TestCase;

final class TracerErrorHandlingTest extends TestCase
{
    public function test_complete_routes_on_end_throwable_to_error_handler(): void
    {
        $processor = $this->createMock(SpanProcessor::class);
        $processor->method('onEnd')->willThrowException(new \RuntimeException('end exploded'));
        $spy = new ErrorHandlerSpy();

        $tracer = new Tracer(
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            $processor,
            new SystemClock(),
            new MemoryContextStorage(),
            errorHandler: $spy,
        );

        $span = $tracer->span('op');
        $tracer->complete($span);

        static::assertSame(1, $spy->count());
        static::assertSame('end exploded', $spy->last()?->getMessage());
    }

    public function test_flush_routes_processor_throwable_to_error_handler(): void
    {
        $processor = $this->createMock(SpanProcessor::class);
        $processor->method('flush')->willThrowException(new \RuntimeException('flush exploded'));
        $spy = new ErrorHandlerSpy();

        $tracer = new Tracer(
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            $processor,
            new SystemClock(),
            new MemoryContextStorage(),
            errorHandler: $spy,
        );

        static::assertFalse($tracer->flush());
        static::assertSame(1, $spy->count());
        static::assertSame('flush exploded', $spy->last()?->getMessage());
    }

    public function test_span_routes_on_start_throwable_to_error_handler(): void
    {
        $processor = $this->createMock(SpanProcessor::class);
        $processor->method('onStart')->willThrowException(new \RuntimeException('start exploded'));
        $spy = new ErrorHandlerSpy();

        $tracer = new Tracer(
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            $processor,
            new SystemClock(),
            new MemoryContextStorage(),
            errorHandler: $spy,
        );

        $tracer->span('op');

        static::assertSame(1, $spy->count());
        static::assertSame('start exploded', $spy->last()?->getMessage());
    }
}
