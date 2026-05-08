<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\{LogProcessor, Logger};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Tests\Mother\{ErrorHandlerSpy, ResourceMother};
use PHPUnit\Framework\TestCase;

final class LoggerErrorHandlingTest extends TestCase
{
    public function test_emit_routes_processor_throwable_to_error_handler() : void
    {
        $processor = $this->createMock(LogProcessor::class);
        $processor->method('process')->willThrowException(new \RuntimeException('processor exploded'));
        $spy = new ErrorHandlerSpy();

        $logger = new Logger(
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            $processor,
            new SystemClock(),
            new MemoryContextStorage(),
            errorHandler: $spy,
        );

        $logger->info('hello world');

        self::assertSame(1, $spy->count());
        self::assertSame('processor exploded', $spy->last()?->getMessage());
    }

    public function test_flush_routes_processor_throwable_to_error_handler() : void
    {
        $processor = $this->createMock(LogProcessor::class);
        $processor->method('flush')->willThrowException(new \RuntimeException('flush exploded'));
        $spy = new ErrorHandlerSpy();

        $logger = new Logger(
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            $processor,
            new SystemClock(),
            new MemoryContextStorage(),
            errorHandler: $spy,
        );

        self::assertFalse($logger->flush());
        self::assertSame(1, $spy->count());
        self::assertSame('flush exploded', $spy->last()?->getMessage());
    }
}
