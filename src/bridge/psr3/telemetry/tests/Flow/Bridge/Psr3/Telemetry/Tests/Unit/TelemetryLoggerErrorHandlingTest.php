<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr3\Telemetry\Tests\Unit;

use Flow\Bridge\Psr3\Telemetry\Exception\InvalidArgumentException;
use Flow\Bridge\Psr3\Telemetry\TelemetryLogger;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;
use Psr\Log\LogLevel;

final class TelemetryLoggerErrorHandlingTest extends TestCase
{
    public function test_continues_emitting_after_normal_log_call() : void
    {
        $loggerProvider = new LoggerProvider(
            new MemoryLogProcessor(new VoidExporter()),
            new SystemClock(),
            new MemoryContextStorage(),
        );

        $logger = $loggerProvider->logger(Resource::empty(), 'test-scope');
        $spy = new ErrorHandlerSpy();
        $psr3 = new TelemetryLogger($logger, errorHandler: $spy);

        $psr3->log(LogLevel::INFO, 'hello');

        self::assertSame(0, $spy->count());
    }

    public function test_invalid_level_is_not_routed_to_error_handler() : void
    {
        $loggerProvider = new LoggerProvider(
            new MemoryLogProcessor(new VoidExporter()),
            new SystemClock(),
            new MemoryContextStorage(),
        );

        $logger = $loggerProvider->logger(Resource::empty(), 'test-scope');
        $spy = new ErrorHandlerSpy();
        $psr3 = new TelemetryLogger($logger, errorHandler: $spy);

        try {
            $psr3->log(new \stdClass(), 'message');
        } catch (InvalidArgumentException) {
        }

        self::assertSame(0, $spy->count());
    }

    public function test_routes_emit_failures_to_error_handler() : void
    {
        $throwingClock = new class implements ClockInterface {
            public function now() : \DateTimeImmutable
            {
                throw new \RuntimeException('clock blew up');
            }
        };

        $loggerProvider = new LoggerProvider(
            new MemoryLogProcessor(new VoidExporter()),
            $throwingClock,
            new MemoryContextStorage(),
        );

        $logger = $loggerProvider->logger(Resource::empty(), 'test-scope');
        $spy = new ErrorHandlerSpy();
        $psr3 = new TelemetryLogger($logger, errorHandler: $spy);

        $psr3->info('hello');

        self::assertSame(1, $spy->count());
        self::assertSame('clock blew up', $spy->last()?->getMessage());
    }

    public function test_still_throws_on_invalid_level_per_psr3() : void
    {
        $loggerProvider = new LoggerProvider(
            new MemoryLogProcessor(new VoidExporter()),
            new SystemClock(),
            new MemoryContextStorage(),
        );

        $logger = $loggerProvider->logger(Resource::empty(), 'test-scope');
        $spy = new ErrorHandlerSpy();
        $psr3 = new TelemetryLogger($logger, errorHandler: $spy);

        $this->expectException(InvalidArgumentException::class);

        $psr3->log(new \stdClass(), 'message');
    }
}
