<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Tracer\SpanContext;
use Psr\Clock\ClockInterface;

/**
 * Logger implementation for emitting log records.
 *
 * A Logger is the primary interface for emitting log telemetry.
 * Log records can include severity, message body, and attributes.
 *
 * Loggers are obtained from a LoggerProvider:
 * ```php
 * $logger = $provider->logger('my-service', '1.0.0');
 * ```
 *
 * Example usage with convenience methods:
 * ```php
 * $logger->info('Processing started', ['items.count' => 100]);
 * $logger->error('Processing failed', ['error.message' => $e->getMessage()]);
 * ```
 */
final class Logger
{
    public function __construct(
        private readonly Resource $resource,
        private InstrumentationScope $scope,
        private readonly LogProcessor $processor,
        private readonly ClockInterface $clock,
        private readonly ContextStorage $contextStorage,
    ) {
    }

    /**
     * Emit a DEBUG level log.
     *
     * Use for diagnostic information useful during development
     * and debugging sessions.
     *
     * @param string $body The log message
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Optional attributes
     */
    public function debug(string $body, array $attributes = []) : void
    {
        $this->emit(new LogRecord(Severity::DEBUG, $body, Attributes::create($attributes)));
    }

    /**
     * Emit a log record.
     *
     * Use this when you need fine-grained control over the log record,
     * such as setting a custom timestamp or recording an exception.
     *
     * @param LogRecord $record The log record to emit
     */
    public function emit(LogRecord $record) : void
    {
        $entry = new LogEntry(
            $record,
            $this->resource,
            $this->scope,
            $record->timestamp ?? $this->clock->now(),
            $this->resolveSpanContext(),
        );

        $this->processor->process($entry);
    }

    /**
     * Emit an ERROR level log.
     *
     * Use when an error occurred but the application can continue
     * running.
     *
     * @param string $body The log message
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Optional attributes
     */
    public function error(string $body, array $attributes = []) : void
    {
        $this->emit(new LogRecord(Severity::ERROR, $body, Attributes::create($attributes)));
    }

    /**
     * Emit a FATAL level log.
     *
     * Use for severe errors that will likely cause the application
     * to terminate or become unusable.
     *
     * @param string $body The log message
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Optional attributes
     */
    public function fatal(string $body, array $attributes = []) : void
    {
        $this->emit(new LogRecord(Severity::FATAL, $body, Attributes::create($attributes)));
    }

    /**
     * Emit an INFO level log.
     *
     * Use for general operational information about the application's
     * normal behavior.
     *
     * @param string $body The log message
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Optional attributes
     */
    public function info(string $body, array $attributes = []) : void
    {
        $this->emit(new LogRecord(Severity::INFO, $body, Attributes::create($attributes)));
    }

    /**
     * Get the instrumentation scope.
     */
    public function instrumentationScope() : InstrumentationScope
    {
        return $this->scope;
    }

    /**
     * Get the processor used by this logger.
     */
    public function processor() : LogProcessor
    {
        return $this->processor;
    }

    /**
     * Emit a TRACE level log.
     *
     * Use for very detailed diagnostic information, typically only
     * enabled during development or troubleshooting.
     *
     * @param string $body The log message
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Optional attributes
     */
    public function trace(string $body, array $attributes = []) : void
    {
        $this->emit(new LogRecord(Severity::TRACE, $body, Attributes::create($attributes)));
    }

    /**
     * Emit a WARN level log.
     *
     * Use for potentially harmful situations that don't prevent
     * the application from functioning.
     *
     * @param string $body The log message
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Optional attributes
     */
    public function warn(string $body, array $attributes = []) : void
    {
        $this->emit(new LogRecord(Severity::WARN, $body, Attributes::create($attributes)));
    }

    /**
     * Change the instrumentation scope for this logger.
     *
     * This mutates the logger instance and returns it for method chaining.
     */
    public function withInstrumentationScope(InstrumentationScope $scope) : self
    {
        $this->scope = $scope;

        return $this;
    }

    private function resolveSpanContext() : ?SpanContext
    {
        $context = $this->contextStorage->current();
        $activeSpanId = $context->activeSpanId();

        if ($activeSpanId === null) {
            return null;
        }

        return SpanContext::create($context->traceId, $activeSpanId);
    }
}
