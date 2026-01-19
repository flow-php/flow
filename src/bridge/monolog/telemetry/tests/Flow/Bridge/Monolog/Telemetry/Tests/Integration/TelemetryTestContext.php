<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Telemetry\Tests\Integration;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\{Logger, LoggerProvider};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidSpanProcessor};
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tracer\{Tracer, TracerProvider};

/**
 * Test context for Telemetry integration tests.
 *
 * Provides pre-configured telemetry components for testing the Monolog-Telemetry bridge.
 */
final readonly class TelemetryTestContext
{
    public function __construct(
        public MemoryLogProcessor $processor,
        public Logger $logger,
        public ?Tracer $tracer = null,
    ) {
    }

    public static function create(
        ?Resource $resource = null,
        string $scope = 'test-app',
        string $version = 'unknown',
    ) : self {
        $contextStorage = new MemoryContextStorage();
        $clock = new SystemClock();
        $resource ??= Resource::create(['service.name' => 'test-service']);
        $processor = new MemoryLogProcessor(new VoidLogExporter());

        $loggerProvider = new LoggerProvider(
            $processor,
            $clock,
            $contextStorage,
        );

        $logger = $loggerProvider->logger(
            $resource,
            $scope,
            $version,
        );

        return new self($processor, $logger);
    }

    /**
     * Create a context with tracing support for testing trace context propagation.
     *
     * Both logger and tracer share the same context storage, enabling
     * automatic trace_id and span_id correlation in log entries.
     */
    public static function createWithTracing(
        ?Resource $resource = null,
        string $scope = 'test-app',
        string $version = 'unknown',
    ) : self {
        $contextStorage = new MemoryContextStorage();
        $clock = new SystemClock();
        $resource ??= Resource::create(['service.name' => 'test-service']);
        $processor = new MemoryLogProcessor(new VoidLogExporter());

        $loggerProvider = new LoggerProvider(
            $processor,
            $clock,
            $contextStorage,
        );

        $tracerProvider = new TracerProvider(
            new VoidSpanProcessor(),
            $clock,
            $contextStorage,
        );

        $logger = $loggerProvider->logger(
            $resource,
            $scope,
            $version,
        );

        $tracer = $tracerProvider->tracer(
            $resource,
            $scope,
            $version,
        );

        return new self($processor, $logger, $tracer);
    }
}
