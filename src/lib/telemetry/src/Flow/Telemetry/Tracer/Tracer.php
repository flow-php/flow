<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\Scope;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tracer\Sampler\Sampler;
use Psr\Clock\ClockInterface;
use Throwable;

/**
 * Creates and manages spans within a trace.
 *
 * Tracer is the primary API for distributed tracing. It creates spans,
 * manages parent-child relationships through the shared Context (the active span lives in the Context and
 * the parent is derived from it), and delegates span lifecycle events to a SpanProcessor.
 *
 * Example usage - a leaf span, which never needs to become the active span:
 * ```php
 * $span = $tracer->span('process-order');
 * try {
 *     // do work
 *     $span->setStatus(SpanStatus::ok());
 * } catch (\Throwable $e) {
 *     $span->recordException($e)->setStatus(SpanStatus::error($e->getMessage()));
 *     throw $e;
 * } finally {
 *     $tracer->complete($span);
 * }
 * ```
 *
 * When spans created inside must nest under this one, activate it and detach the scope before completing:
 * ```php
 * $span = $tracer->span('process-order');
 * $scope = $tracer->activate($span);
 * try {
 *     // spans started here become children of $span
 * } finally {
 *     $scope->detach();
 *     $tracer->complete($span);
 * }
 * ```
 *
 * Or use the trace() helper:
 * ```php
 * $result = $tracer->trace('process-order', function() {
 *     // do work
 *     return $result;
 * });
 * ```
 *
 * @phpstan-import-type TAttributeValueMap from Attributes
 */
final class Tracer
{
    public function __construct(
        private readonly Resource $resource,
        private InstrumentationScope $scope,
        private readonly SpanProcessor $processor,
        private readonly ClockInterface $clock,
        private readonly ContextStorage $contextStorage,
        private readonly ?Sampler $sampler = null,
        private readonly SpanLimits $limits = new SpanLimits(),
        private readonly ErrorHandler $errorHandler = new ErrorLogHandler(),
        private readonly Attributes $signalAttributes = new Attributes(),
    ) {}

    /**
     * Make a span the active span in the current Context.
     *
     * The returned Scope must be detached by the caller, in LIFO order, before the span is completed.
     */
    public function activate(Span $span): Scope
    {
        return $this->contextStorage->attach($this->contextStorage->current()->withActiveSpan($span->context()));
    }

    /**
     * Get the currently active span context from the shared Context, or null if none.
     */
    public function activeSpan(): ?SpanContext
    {
        return $this->contextStorage->current()->activeSpan();
    }

    /**
     * Complete a span and pass it to the processor.
     *
     * This ends the span (if not already ended) and notifies the processor. Detaching the Context scope is the
     * responsibility of whoever called activate().
     */
    public function complete(Span $span): void
    {
        // OTEL spec: End "MUST be called only once per span"; subsequent calls are ignored rather than
        // re-exporting. isEnded() alone is not the guard - callers may end a span to read its duration first.
        if ($span->isCompleted()) {
            return;
        }

        if (!$span->isEnded()) {
            $span->end($this->clock->now());
        }

        $span->markCompleted();

        if ($span->isRecording()) {
            try {
                $this->processor->onEnd($span);
            } catch (Throwable $e) {
                $this->errorHandler->handle($e);
            }
        }
    }

    /**
     * Get the tracer's context.
     */
    public function context(): Context
    {
        return $this->contextStorage->current();
    }

    /**
     * Flush all pending spans to the exporter.
     */
    public function flush(): bool
    {
        try {
            return $this->processor->flush();
        } catch (Throwable $e) {
            $this->errorHandler->handle($e);

            return false;
        }
    }

    /**
     * Get the instrumentation scope.
     */
    public function instrumentationScope(): InstrumentationScope
    {
        return $this->scope;
    }

    /**
     * Get the tracer name.
     */
    public function name(): string
    {
        return $this->scope->name;
    }

    /**
     * Get the processor used by this tracer.
     */
    public function processor(): SpanProcessor
    {
        return $this->processor;
    }

    /**
     * Start a new span, without making it the active span.
     *
     * OTEL trace API: "Span creation MUST NOT set the newly created Span as the active Span in the current
     * Context by default, but this functionality MAY be offered additionally as a separate operation."
     * That separate operation is activate().
     *
     * @param string $name The span name
     * @param SpanKind $kind The span kind
     * @param TAttributeValueMap|Attributes $attributes Initial attributes
     * @param array<SpanLink> $links Links to other spans
     * @param null|false|Context $parent Explicit parent control:
     *                                   - null (default): automatic detection from the current Context's active span
     *                                   - Context: use that Context's active span as parent
     *                                   - false: create root span (no parent)
     */
    public function span(
        string $name,
        SpanKind $kind = SpanKind::INTERNAL,
        Attributes|array $attributes = [],
        array $links = [],
        Context|false|null $parent = null,
    ): Span {
        $context = $this->contextStorage->current();

        // OTEL spec: the sampler receives the parent Context, not the ambient one. Dropping only the active
        // span for a root span keeps suppression and baggage, which are not parent-child properties.
        $parentContext = match (true) {
            $parent === false => $context->withoutActiveSpan(),
            $parent !== null => $parent,
            default => $context,
        };

        $parentSpanContext = $parentContext->activeSpan();

        // OpenTelemetry: a span inherits its parent's trace id; a root span (no parent) starts a new trace.
        $traceId = $parentSpanContext !== null ? $parentSpanContext->traceId : TraceId::generate();
        $parentSpanId = $parentSpanContext?->spanId;
        $parentIsRemote = $parentSpanContext !== null && $parentSpanContext->isRemote;

        $spanId = SpanId::generate();
        $traceFlags = $parentSpanContext !== null ? $parentSpanContext->traceFlags : TraceFlags::sampled();
        $traceState = $parentSpanContext?->traceState;
        $isRecording = true;

        $spanContext = $parentIsRemote
            ? SpanContext::createRemote($traceId, $spanId, $parentSpanId, $traceFlags, $traceState)
            : SpanContext::create($traceId, $spanId, $parentSpanId, $traceFlags, $traceState);

        $startTime = $this->clock->now();
        $span = new Span(
            $name,
            $spanContext,
            $kind,
            $startTime,
            $this->resource,
            $this->scope,
            $isRecording,
            $this->limits,
        );

        $attributesToSet = $attributes instanceof Attributes ? $attributes : Attributes::create($attributes);

        if (!$this->signalAttributes->isEmpty()) {
            $attributesToSet = $this->signalAttributes->merge($attributesToSet);
        }

        $span->setAttributes($attributesToSet);

        foreach ($links as $link) {
            $span->addLink($link);
        }

        if ($this->sampler !== null) {
            $samplingResult = $this->sampler->shouldSample($parentContext, $span);

            $isRecording = $samplingResult->decision->isRecording();
            $traceFlags = $samplingResult->decision->isSampled() ? TraceFlags::sampled() : TraceFlags::default();

            if ($samplingResult->traceState !== null) {
                $traceState = $samplingResult->traceState;
            }

            $attributesToSet = $attributesToSet->merge(Attributes::create($samplingResult->attributes));

            if (!$isRecording || !$samplingResult->decision->isSampled()) {
                $spanContext = $parentIsRemote
                    ? SpanContext::createRemote($traceId, $spanId, $parentSpanId, $traceFlags, $traceState)
                    : SpanContext::create($traceId, $spanId, $parentSpanId, $traceFlags, $traceState);

                $span = new Span(
                    $name,
                    $spanContext,
                    $kind,
                    $startTime,
                    $this->resource,
                    $this->scope,
                    $isRecording,
                    $this->limits,
                );
                $span->setAttributes($attributesToSet);

                foreach ($links as $link) {
                    $span->addLink($link);
                }
            }
        }

        if ($span->isRecording()) {
            try {
                $this->processor->onStart($span);
            } catch (Throwable $e) {
                $this->errorHandler->handle($e);
            }
        }

        return $span;
    }

    /**
     * Execute a callable within a span, automatically completing it.
     *
     * The span is automatically completed after the callback finishes,
     * regardless of whether it throws an exception. If an exception is
     * thrown, it is recorded as an event and the status is set to error.
     *
     * @template T
     *
     * @param string $name The span name
     * @param callable(): T $callback The callable to execute
     * @param SpanKind $kind The span kind
     * @param null|false|Context $parent Explicit parent control (see span() for details)
     *
     * @throws \Throwable Rethrows any exception from the callback
     *
     * @return T The callback result
     */
    public function trace(
        string $name,
        callable $callback,
        SpanKind $kind = SpanKind::INTERNAL,
        Context|false|null $parent = null,
    ): mixed {
        $span = $this->span($name, $kind, [], [], $parent);
        $scope = $this->activate($span);

        try {
            // OTEL spec: instrumentation leaves the status Unset on success; only errors set a status.
            return $callback();
        } catch (Throwable $e) {
            $span->recordException($e, $this->clock->now());
            $span->setAttribute('error.type', $e::class);
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
            $scope->detach();
            $this->complete($span);
        }
    }

    /**
     * Get the tracer version.
     */
    public function version(): string
    {
        return $this->scope->version;
    }

    /**
     * Change the instrumentation scope for this tracer.
     *
     * This mutates the tracer instance and returns it for method chaining.
     */
    public function withInstrumentationScope(InstrumentationScope $scope): self
    {
        $this->scope = $scope;

        return $this;
    }
}
