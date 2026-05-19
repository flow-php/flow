<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter;

use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Meter\Meter;
use Flow\Telemetry\Meter\MetricProcessor;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class MeterErrorHandlingTest extends TestCase
{
    public function test_complete_routes_processor_throwable_to_error_handler(): void
    {
        $processor = $this->createMock(MetricProcessor::class);
        $processor->method('process')->willThrowException(new RuntimeException('process exploded'));
        $spy = new ErrorHandlerSpy();

        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            $processor,
            new SystemClock(),
            errorHandler: $spy,
        );

        $counter = $meter->createCounter('test.counter');
        $counter->add(1);

        $meter->complete($counter);

        static::assertSame(1, $spy->count());
        static::assertSame('process exploded', $spy->last()?->getMessage());
    }

    public function test_flush_routes_processor_throwable_to_error_handler(): void
    {
        $processor = $this->createMock(MetricProcessor::class);
        $processor->method('flush')->willThrowException(new RuntimeException('flush exploded'));
        $spy = new ErrorHandlerSpy();

        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            $processor,
            new SystemClock(),
            errorHandler: $spy,
        );

        static::assertFalse($meter->flush());
        static::assertSame(1, $spy->count());
        static::assertSame('flush exploded', $spy->last()?->getMessage());
    }
}
