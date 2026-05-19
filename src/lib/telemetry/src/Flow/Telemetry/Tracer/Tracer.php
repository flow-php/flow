<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tracer\Sampler\Sampler;
use Psr\Clock\ClockInterface;
use SplStack;
use Throwable;

/**
 * Creates and manages spans within a trace.
 *
 * Tracer is the primary API for distributed tracing. It creates spans,
 * manages parent-child relationships through a span stack, and delegates
 * span lifecycle events to a SpanProcessor.
 *
 * Example usage:
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
 * Or use the trace() helper:
 * ```php
 * $result = $tracer->trace('process-order', function() {
 *     // do work
 *     return $result;
 * });
 * ```
 */
final class Tracer
{
    /**
     * @var \SplStack<SpanContext>
     */
    private readonly SplStack $spanStack;

    public function __construct(
        private readonly Resource $resource,
        private InstrumentationScope $scope,
        private readonly SpanProcessor $processor,
        private readonly ClockInterface $clock,
        private readonly ContextStorage $contextStorage,
        private readonly ?Sampler $sampler = null,
        private readonly SpanLimits $limits = new SpanLimits(),
        private readonly ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {
        /** @var \SplStack<SpanContext> $stack */
        $stack = new SplStack();
        $this->spanStack = $stack;
    }

    /**
     * Get the currently active span context, or null if none.
     */
    public function activeSpan(): ?SpanContext
    {
        if ($this->spanStack->isEmpty()) {
            return null;
        }

        return $this->spanStack->top();
    }

    /**
     * Complete a span and pass it to the processor.
     *
     * This ends the span (if not already ended), removes it from the
     * active span stack, and notifies the processor.
     */
    public function complete(Span $span): void
    {
        if (!$span->isEnded()) {
            $span->end($this->clock->now());
        }

        if (!$this->spanStack->isEmpty()) {
            $topContext = $this->spanStack->top();

            if ($topContext->spanId->equals($span->context()->spanId)) {
                $this->spanStack->pop();
            }
        }

        $span->contextScope()?->detach();

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
     * Start a new span.
     *
     * If there's an active span, it becomes the parent of the new span.
     * The new span is pushed onto the stack and becomes the active span.
     *
     * @param string $name The span name
     * @param SpanKind $kind The span kind
     * @param array<string, array<bool|float|int|string>|bool|float|int|string>|Attributes $attributes Initial attributes
     * @param array<SpanLink> $links Links to other spans
     * @param null|false|SpanContext $parentContext Explicit parent control:
     *                                              - null (default): automatic detection from stack/context
     *                                              - SpanContext: use as explicit parent
     *                                              - false: create root span (no parent)
     */
    public function span(
        string $name,
        SpanKind $kind = SpanKind::INTERNAL,
        Attributes|array $attributes = [],
        array $links = [],
        SpanContext|false|null $parentContext = null,
    ): Span {
        $context = $this->contextStorage->current();
        $parentSpanId = null;
        $parentSpanContext = null;
        $parentIsRemote = false;

        if ($parentContext === false) {
            $parentSpanId = null;
            $parentSpanContext = null;
        } elseif ($parentContext !== null) {
            $parentSpanContext = $parentContext;
            $parentSpanId = $parentContext->spanId;
            $parentIsRemote = $parentContext->isRemote;
        } elseif (!$this->spanStack->isEmpty()) {
            $parentSpanContext = $this->spanStack->top();
            $parentSpanId = $parentSpanContext->spanId;
        } elseif ($context->activeSpanId() !== null) {
            $parentSpanId = $context->activeSpanId();
            $parentIsRemote = true;
        }

        $traceId = $context->traceId;

        if (!$traceId->isValid()) {
            $traceId = TraceId::generate();
            $context = Context::withTraceId($traceId);
            $this->contextStorage->attach($context);
        }

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
        $span->setAttributes($attributesToSet);

        foreach ($links as $link) {
            $span->addLink($link);
        }

        if ($this->sampler !== null) {
            $samplingResult = $this->sampler->shouldSample($span);

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

        $this->spanStack->push($span->context());
        $span->setContextScope($this->contextStorage->attach($context->withActiveSpan($span->context()->spanId)));

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
     * @param null|false|SpanContext $parentContext Explicit parent control (see span() for details)
     *
     * @throws \Throwable Rethrows any exception from the callback
     *
     * @return T The callback result
     */
    public function trace(
        string $name,
        callable $callback,
        SpanKind $kind = SpanKind::INTERNAL,
        SpanContext|false|null $parentContext = null,
    ): mixed {
        $span = $this->span($name, $kind, [], [], $parentContext);

        try {
            $result = $callback();
            $span->setStatus(SpanStatus::ok());

            return $result;
        } catch (Throwable $e) {
            $span->recordException($e, $this->clock->now());
            $span->setStatus(SpanStatus::error($e->getMessage()));

            throw $e;
        } finally {
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
