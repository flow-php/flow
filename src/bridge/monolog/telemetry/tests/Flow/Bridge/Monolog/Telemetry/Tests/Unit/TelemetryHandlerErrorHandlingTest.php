<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry\Tests\Unit;

use Flow\Bridge\Monolog\Telemetry\TelemetryHandler;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Monolog\{Level, Logger as MonologLogger};
use PHPUnit\Framework\TestCase;
use Psr\Clock\ClockInterface;

final class TelemetryHandlerErrorHandlingTest extends TestCase
{
    public function test_does_not_propagate_exception_to_monolog_caller() : void
    {
        $this->expectNotToPerformAssertions();

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
        $handler = new TelemetryHandler($logger, level: Level::Debug, errorHandler: $spy);

        $monolog = new MonologLogger('test-channel');
        $monolog->pushHandler($handler);

        $monolog->info('hello');
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
        $handler = new TelemetryHandler($logger, errorHandler: $spy);

        $monolog = new MonologLogger('test-channel');
        $monolog->pushHandler($handler);

        $monolog->info('hello');

        self::assertSame(1, $spy->count());
        self::assertSame('clock blew up', $spy->last()?->getMessage());
    }
}
