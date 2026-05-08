<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter;

use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Meter\{Meter, MetricProcessor};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Tests\Mother\{ErrorHandlerSpy, ResourceMother};
use PHPUnit\Framework\TestCase;

final class MeterErrorHandlingTest extends TestCase
{
    public function test_complete_routes_processor_throwable_to_error_handler() : void
    {
        $processor = $this->createMock(MetricProcessor::class);
        $processor->method('process')->willThrowException(new \RuntimeException('process exploded'));
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

        self::assertSame(1, $spy->count());
        self::assertSame('process exploded', $spy->last()?->getMessage());
    }

    public function test_flush_routes_processor_throwable_to_error_handler() : void
    {
        $processor = $this->createMock(MetricProcessor::class);
        $processor->method('flush')->willThrowException(new \RuntimeException('flush exploded'));
        $spy = new ErrorHandlerSpy();

        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            $processor,
            new SystemClock(),
            errorHandler: $spy,
        );

        self::assertFalse($meter->flush());
        self::assertSame(1, $spy->count());
        self::assertSame('flush exploded', $spy->last()?->getMessage());
    }
}
