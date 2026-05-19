<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Logger;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Logger\LogProcessor;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class LoggerErrorHandlingTest extends TestCase
{
    public function test_emit_routes_processor_throwable_to_error_handler(): void
    {
        $processor = $this->createMock(LogProcessor::class);
        $processor->method('process')->willThrowException(new RuntimeException('processor exploded'));
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

        static::assertSame(1, $spy->count());
        static::assertSame('processor exploded', $spy->last()?->getMessage());
    }

    public function test_flush_routes_processor_throwable_to_error_handler(): void
    {
        $processor = $this->createMock(LogProcessor::class);
        $processor->method('flush')->willThrowException(new RuntimeException('flush exploded'));
        $spy = new ErrorHandlerSpy();

        $logger = new Logger(
            ResourceMother::default(),
            new InstrumentationScope('test', '1.0.0'),
            $processor,
            new SystemClock(),
            new MemoryContextStorage(),
            errorHandler: $spy,
        );

        static::assertFalse($logger->flush());
        static::assertSame(1, $spy->count());
        static::assertSame('flush exploded', $spy->last()?->getMessage());
    }
}
