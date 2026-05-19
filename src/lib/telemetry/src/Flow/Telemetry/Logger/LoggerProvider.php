<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Resource;
use Psr\Clock\ClockInterface;

/**
 * Entry point for the logging API.
 *
 * Creates and manages loggers for different instrumentation scopes.
 * The behavior (void, memory, OTLP, etc.) is determined by the injected processor.
 *
 * Example usage:
 * ```php
 * // For testing with memory storage
 * $processor = new MemoryLogProcessor(new MemoryExporter());
 * $provider = new LoggerProvider($processor, new SystemClock());
 *
 * // For OTLP export
 * $processor = batching_log_processor(otlp_exporter($transport));
 * $provider = new LoggerProvider($processor, new SystemClock());
 *
 * // For void/disabled logging
 * $provider = new LoggerProvider(new VoidProcessor(), new SystemClock());
 *
 * $logger = $provider->logger('my-service', '1.0.0');
 * $logger->info('Application started');
 * ```
 */
final readonly class LoggerProvider
{
    public function __construct(
        private LogProcessor $processor,
        private ClockInterface $clock,
        private ContextStorage $contextStorage,
        private LogRecordLimits $limits = new LogRecordLimits(),
        private ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {}

    /**
     * Create a logger for the given instrumentation scope.
     *
     * Note: Instance caching is handled by the Telemetry class.
     * This method always creates a new Logger instance.
     *
     * @param \Flow\Telemetry\Resource $resource The resource context for all logs from this logger
     * @param string $name The name of the instrumentation scope (e.g., library name)
     * @param string $version The version of the instrumentation scope
     * @param null|string $schemaUrl Schema URL for semantic conventions
     * @param null|Attributes $attributes Additional scope attributes
     */
    public function logger(
        Resource $resource,
        string $name,
        string $version = 'unknown',
        ?string $schemaUrl = null,
        ?Attributes $attributes = null,
    ): Logger {
        return new Logger(
            $resource,
            new InstrumentationScope($name, $version, $schemaUrl, $attributes ?? new Attributes()),
            $this->processor,
            $this->clock,
            $this->contextStorage,
            $this->limits,
            $this->errorHandler,
        );
    }
}
