<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

use Flow\Telemetry\{Attributes, InstrumentationScope, Resource};

/**
 * Represents a single operation within a trace.
 *
 * A Span is the primary building block of a trace, representing a single
 * operation with a start time, end time, attributes, events, and links.
 * Spans are mutable during their lifetime and can accumulate state.
 *
 * Example usage:
 * ```php
 * $span = new Span('process-order', $context, SpanKind::INTERNAL, new \DateTimeImmutable());
 * $span->setAttribute('order.id', '12345')
 *      ->recordEvent(GenericEvent::create('validation.passed', new \DateTimeImmutable()))
 *      ->setStatus(SpanStatus::ok())
 *      ->end();
 * ```
 *
 * @see https://opentelemetry.io/docs/specs/otel/trace/api/#span
 */
final class Span
{
    private Attributes $attributes;

    private ?\DateTimeImmutable $endTime = null;

    /**
     * @var array<SpanEvent>
     */
    private array $events = [];

    /**
     * @var array<SpanLink>
     */
    private array $links = [];

    private ?SpanStatus $status = null;

    public function __construct(
        private string $name,
        private readonly SpanContext $context,
        private readonly SpanKind $kind,
        private readonly \DateTimeImmutable $startTime,
        private readonly Resource $resource,
        private readonly InstrumentationScope $scope,
        private readonly bool $isRecording = true,
    ) {
        $this->attributes = Attributes::empty();
    }

    /**
     * Create a Span from a normalized array representation.
     *
     * Note: The returned span is always in an ended state (isRecording = false)
     * since it represents a completed span that was serialized.
     *
     * @param array{
     *     name: string,
     *     context: array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags: array{byte: int}, traceState: array{entries: array<string, string>}},
     *     kind: string,
     *     startTime: string,
     *     endTime: null|string,
     *     resource: array{attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     scope: array{name: string, version: string, schemaUrl: null|string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     attributes: array<string, array<bool|float|int|string>|bool|float|int|string>,
     *     events: array<array{name: string, timestamp: string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>}>,
     *     links: array<array{context: array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool}, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>}>,
     *     status: null|array{code: int, description: null|string},
     *     isRecording: bool
     * } $data Normalized Span data
     */
    public static function fromArray(array $data) : self
    {
        $span = new self(
            $data['name'],
            SpanContext::fromArray($data['context']),
            SpanKind::from($data['kind']),
            new \DateTimeImmutable($data['startTime']),
            Resource::fromArray($data['resource']),
            InstrumentationScope::fromArray($data['scope']),
            $data['isRecording'],
        );

        $span->attributes = Attributes::fromArray($data['attributes']);

        foreach ($data['events'] as $eventData) {
            $span->events[] = GenericEvent::fromArray($eventData);
        }

        foreach ($data['links'] as $linkData) {
            $span->links[] = SpanLink::fromArray($linkData);
        }

        if ($data['status'] !== null) {
            $span->status = SpanStatus::fromArray($data['status']);
        }

        if ($data['endTime'] !== null) {
            $span->endTime = new \DateTimeImmutable($data['endTime']);
        }

        return $span;
    }

    /**
     * Add a link to another span.
     *
     * @return $this
     */
    public function addLink(SpanLink $link) : self
    {
        $this->links[] = $link;

        return $this;
    }

    /**
     * Get all span attributes as array.
     *
     * @return array<string, array<bool|float|int|string>|bool|float|int|string>
     */
    public function attributes() : array
    {
        return $this->attributes->normalize();
    }

    /**
     * Get the span attributes as an Attributes object.
     */
    public function attributesObject() : Attributes
    {
        return $this->attributes;
    }

    /**
     * Get the span context.
     */
    public function context() : SpanContext
    {
        return $this->context;
    }

    /**
     * Get the span duration in milliseconds.
     *
     * Returns null if the span has not ended yet.
     */
    public function duration() : ?float
    {
        if ($this->endTime === null) {
            return null;
        }

        $startMicros = (float) $this->startTime->format('U.u');
        $endMicros = (float) $this->endTime->format('U.u');

        return ($endMicros - $startMicros) * 1000;
    }

    /**
     * End the span, optionally with a specific end time.
     *
     * If no end time is provided, the current time is used.
     *
     * @return $this
     */
    public function end(?\DateTimeImmutable $endTime = null) : self
    {
        if ($this->endTime === null) {
            $this->endTime = $endTime ?? new \DateTimeImmutable();
        }

        return $this;
    }

    /**
     * Get the span end time, if ended.
     */
    public function endTime() : ?\DateTimeImmutable
    {
        return $this->endTime;
    }

    /**
     * Get all recorded events.
     *
     * @return array<SpanEvent>
     */
    public function events() : array
    {
        return $this->events;
    }

    /**
     * Check if the span has ended.
     */
    public function isEnded() : bool
    {
        return $this->endTime !== null;
    }

    /**
     * Check if this span is recording events, attributes, and links.
     *
     * Non-recording spans are created when sampling decisions indicate
     * the span should not be exported. Non-recording spans still propagate
     * context but don't record any data.
     */
    public function isRecording() : bool
    {
        return $this->isRecording;
    }

    /**
     * Get the span kind.
     */
    public function kind() : SpanKind
    {
        return $this->kind;
    }

    /**
     * Get all span links.
     *
     * @return array<SpanLink>
     */
    public function links() : array
    {
        return $this->links;
    }

    /**
     * Get the span name.
     */
    public function name() : string
    {
        return $this->name;
    }

    /**
     * Normalize the Span to an array representation for serialization.
     *
     * @return array{
     *     name: string,
     *     context: array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags: array{byte: int}, traceState: array{entries: array<string, string>}},
     *     kind: string,
     *     startTime: string,
     *     endTime: null|string,
     *     resource: array{attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     scope: array{name: string, version: string, schemaUrl: null|string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     attributes: array<string, array<bool|float|int|string>|bool|float|int|string>,
     *     events: array<array{name: string, timestamp: string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>}>,
     *     links: array<array{context: array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool}, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>}>,
     *     status: null|array{code: int, description: null|string},
     *     isRecording: bool
     * }
     */
    public function normalize() : array
    {
        $events = [];

        foreach ($this->events as $event) {
            $events[] = $event->normalize();
        }

        $links = [];

        foreach ($this->links as $link) {
            $links[] = $link->normalize();
        }

        return [
            'name' => $this->name,
            'context' => $this->context->normalize(),
            'kind' => $this->kind->value,
            'startTime' => $this->startTime->format('c'),
            'endTime' => $this->endTime?->format('c'),
            'resource' => $this->resource->normalize(),
            'scope' => $this->scope->normalize(),
            'attributes' => $this->attributes->normalize(),
            'events' => $events,
            'links' => $links,
            'status' => $this->status?->normalize(),
            'isRecording' => $this->isRecording,
        ];
    }

    /**
     * Record an event in this span.
     *
     * @return $this
     */
    public function recordEvent(SpanEvent $event) : self
    {
        $this->events[] = $event;

        return $this;
    }

    /**
     * Record an exception as an event.
     *
     * Creates an event with OpenTelemetry semantic conventions for exceptions.
     *
     * @param \Throwable $exception The exception to record
     * @param \DateTimeImmutable $timestamp The timestamp when the exception occurred
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes Additional attributes
     *
     * @return $this
     */
    public function recordException(\Throwable $exception, \DateTimeImmutable $timestamp, array $attributes = []) : self
    {
        $eventAttributes = \array_merge([
            'exception.type' => $exception::class,
            'exception.message' => $exception->getMessage(),
            'exception.stacktrace' => $exception->getTraceAsString(),
        ], $attributes);

        $this->events[] = GenericEvent::create('exception', $timestamp, $eventAttributes);

        return $this;
    }

    /**
     * Rename the span.
     *
     * @return $this
     */
    public function rename(string $name) : self
    {
        $this->name = $name;

        return $this;
    }

    /**
     * Get the resource describing the entity producing telemetry.
     */
    public function resource() : Resource
    {
        return $this->resource;
    }

    /**
     * Get the instrumentation scope that created this span.
     */
    public function scope() : InstrumentationScope
    {
        return $this->scope;
    }

    /**
     * Set a single attribute.
     *
     * @param array<bool|\DateTimeInterface|float|int|string|\Throwable>|bool|\DateTimeInterface|float|int|string|\Throwable $value
     *
     * @return $this
     */
    public function setAttribute(string $key, string|int|float|bool|\DateTimeInterface|\Throwable|array $value) : self
    {
        $this->attributes = $this->attributes->with($key, $value);

        return $this;
    }

    /**
     * Set multiple attributes at once.
     *
     * @param array<string, array<bool|\DateTimeInterface|float|int|string|\Throwable>|bool|\DateTimeInterface|float|int|string|\Throwable> $attributes
     *
     * @return $this
     */
    public function setAttributes(array $attributes) : self
    {
        $this->attributes = $this->attributes->merge(Attributes::create($attributes));

        return $this;
    }

    /**
     * Set the span status.
     *
     * @return $this
     */
    public function setStatus(SpanStatus $status) : self
    {
        $this->status = $status;

        return $this;
    }

    /**
     * Get the span start time.
     */
    public function startTime() : \DateTimeImmutable
    {
        return $this->startTime;
    }

    /**
     * Get the span status.
     */
    public function status() : ?SpanStatus
    {
        return $this->status;
    }
}
