<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

use Flow\Telemetry\{AttributeLimitsEnforcer, Attributes};
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\ErrorHandler\{ErrorHandler, ErrorLogHandler};
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
 *
 * @phpstan-import-type TAttributeValue from Attributes
 * @phpstan-import-type TAttributeValueMap from Attributes
 */
final class Logger
{
    public function __construct(
        private readonly Resource $resource,
        private InstrumentationScope $scope,
        private readonly LogProcessor $processor,
        private readonly ClockInterface $clock,
        private readonly ContextStorage $contextStorage,
        private readonly LogRecordLimits $limits = new LogRecordLimits(),
        private readonly ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {
    }

    /**
     * Emit a DEBUG level log.
     *
     * Use for diagnostic information useful during development
     * and debugging sessions.
     *
     * @param string $body The log message
     * @param Attributes|TAttributeValueMap $attributes Optional attributes
     * @param null|\DateTimeImmutable $timestamp When the event occurred (defaults to current time)
     * @param null|\DateTimeImmutable $observedTimestamp When the log was observed by collection
     * @param null|SpanContext $spanContext Span context for trace correlation (defaults to current active span)
     */
    public function debug(
        string $body,
        array|Attributes $attributes = [],
        ?\DateTimeImmutable $timestamp = null,
        ?\DateTimeImmutable $observedTimestamp = null,
        ?SpanContext $spanContext = null,
    ) : void {
        $this->emit(
            new LogRecord(
                Severity::DEBUG,
                $body,
                $attributes instanceof Attributes ? $attributes : Attributes::create($attributes),
                $timestamp,
                $observedTimestamp,
            ),
            $spanContext,
        );
    }

    /**
     * Emit a log record.
     *
     * Use this when you need fine-grained control over the log record,
     * such as setting a custom timestamp or recording an exception.
     *
     * @param LogRecord $record The log record to emit
     * @param null|SpanContext $spanContext Span context for trace correlation (defaults to current active span)
     */
    public function emit(LogRecord $record, ?SpanContext $spanContext = null) : void
    {
        $droppedAttributeCount = 0;

        if ($record->attributes->count() > $this->limits->attributeCountLimit || $this->limits->attributeValueLengthLimit !== null) {
            $enforcer = new AttributeLimitsEnforcer();
            $result = $enforcer->enforce(
                $record->attributes,
                $this->limits->attributeCountLimit,
                $this->limits->attributeValueLengthLimit,
            );

            $record = new LogRecord(
                $record->severity,
                $record->body,
                $result->attributes,
                $record->timestamp,
                $record->observedTimestamp,
            );
            $droppedAttributeCount = $result->droppedAttributeCount;
        }

        $entry = new LogEntry(
            $record,
            $this->resource,
            $this->scope,
            $record->timestamp ?? $this->clock->now(),
            $spanContext ?? $this->resolveSpanContext(),
            $droppedAttributeCount,
        );

        try {
            $this->processor->process($entry);
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }

    /**
     * Emit an ERROR level log.
     *
     * Use when an error occurred but the application can continue
     * running.
     *
     * @param string $body The log message
     * @param Attributes|TAttributeValueMap $attributes Optional attributes
     * @param null|\DateTimeImmutable $timestamp When the event occurred (defaults to current time)
     * @param null|\DateTimeImmutable $observedTimestamp When the log was observed by collection
     * @param null|SpanContext $spanContext Span context for trace correlation (defaults to current active span)
     */
    public function error(
        string $body,
        array|Attributes $attributes = [],
        ?\DateTimeImmutable $timestamp = null,
        ?\DateTimeImmutable $observedTimestamp = null,
        ?SpanContext $spanContext = null,
    ) : void {
        $this->emit(
            new LogRecord(
                Severity::ERROR,
                $body,
                $attributes instanceof Attributes ? $attributes : Attributes::create($attributes),
                $timestamp,
                $observedTimestamp,
            ),
            $spanContext,
        );
    }

    /**
     * Emit a FATAL level log.
     *
     * Use for severe errors that will likely cause the application
     * to terminate or become unusable.
     *
     * @param string $body The log message
     * @param Attributes|TAttributeValueMap $attributes Optional attributes
     * @param null|\DateTimeImmutable $timestamp When the event occurred (defaults to current time)
     * @param null|\DateTimeImmutable $observedTimestamp When the log was observed by collection
     * @param null|SpanContext $spanContext Span context for trace correlation (defaults to current active span)
     */
    public function fatal(
        string $body,
        array|Attributes $attributes = [],
        ?\DateTimeImmutable $timestamp = null,
        ?\DateTimeImmutable $observedTimestamp = null,
        ?SpanContext $spanContext = null,
    ) : void {
        $this->emit(
            new LogRecord(
                Severity::FATAL,
                $body,
                $attributes instanceof Attributes ? $attributes : Attributes::create($attributes),
                $timestamp,
                $observedTimestamp,
            ),
            $spanContext,
        );
    }

    /**
     * Flush all pending logs to the exporter.
     */
    public function flush() : bool
    {
        try {
            return $this->processor->flush();
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);

            return false;
        }
    }

    /**
     * Emit an INFO level log.
     *
     * Use for general operational information about the application's
     * normal behavior.
     *
     * @param string $body The log message
     * @param Attributes|TAttributeValueMap $attributes Optional attributes
     * @param null|\DateTimeImmutable $timestamp When the event occurred (defaults to current time)
     * @param null|\DateTimeImmutable $observedTimestamp When the log was observed by collection
     * @param null|SpanContext $spanContext Span context for trace correlation (defaults to current active span)
     */
    public function info(
        string $body,
        array|Attributes $attributes = [],
        ?\DateTimeImmutable $timestamp = null,
        ?\DateTimeImmutable $observedTimestamp = null,
        ?SpanContext $spanContext = null,
    ) : void {
        $this->emit(
            new LogRecord(
                Severity::INFO,
                $body,
                $attributes instanceof Attributes ? $attributes : Attributes::create($attributes),
                $timestamp,
                $observedTimestamp,
            ),
            $spanContext,
        );
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
     * @param Attributes|TAttributeValueMap $attributes Optional attributes
     * @param null|\DateTimeImmutable $timestamp When the event occurred (defaults to current time)
     * @param null|\DateTimeImmutable $observedTimestamp When the log was observed by collection
     * @param null|SpanContext $spanContext Span context for trace correlation (defaults to current active span)
     */
    public function trace(
        string $body,
        array|Attributes $attributes = [],
        ?\DateTimeImmutable $timestamp = null,
        ?\DateTimeImmutable $observedTimestamp = null,
        ?SpanContext $spanContext = null,
    ) : void {
        $this->emit(
            new LogRecord(
                Severity::TRACE,
                $body,
                $attributes instanceof Attributes ? $attributes : Attributes::create($attributes),
                $timestamp,
                $observedTimestamp,
            ),
            $spanContext,
        );
    }

    /**
     * Emit a WARN level log.
     *
     * Use for potentially harmful situations that don't prevent
     * the application from functioning.
     *
     * @param string $body The log message
     * @param Attributes|TAttributeValueMap $attributes Optional attributes
     * @param null|\DateTimeImmutable $timestamp When the event occurred (defaults to current time)
     * @param null|\DateTimeImmutable $observedTimestamp When the log was observed by collection
     * @param null|SpanContext $spanContext Span context for trace correlation (defaults to current active span)
     */
    public function warn(
        string $body,
        array|Attributes $attributes = [],
        ?\DateTimeImmutable $timestamp = null,
        ?\DateTimeImmutable $observedTimestamp = null,
        ?SpanContext $spanContext = null,
    ) : void {
        $this->emit(
            new LogRecord(
                Severity::WARN,
                $body,
                $attributes instanceof Attributes ? $attributes : Attributes::create($attributes),
                $timestamp,
                $observedTimestamp,
            ),
            $spanContext,
        );
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

        if ($activeSpanId === null || !$context->traceId->isValid()) {
            return null;
        }

        return SpanContext::create($context->traceId, $activeSpanId);
    }
}
