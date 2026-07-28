<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

use DateTimeImmutable;
use DateTimeInterface;
use Flow\Telemetry\AttributeLimitsEnforcer;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Resource;
use Throwable;

use function array_map;
use function count;
use function is_array;
use function is_string;
use function mb_strlen;
use function mb_substr;

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
 *
 * @phpstan-import-type TAttributeValue from Attributes
 * @phpstan-import-type TAttributeValueMap from Attributes
 */
final class Span
{
    private Attributes $attributes;

    private bool $completed = false;

    private int $droppedAttributeCount = 0;

    private int $droppedEventsCount = 0;

    private int $droppedLinksCount = 0;

    private ?DateTimeImmutable $endTime = null;

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
        private readonly DateTimeImmutable $startTime,
        private readonly Resource $resource,
        private readonly InstrumentationScope $scope,
        private readonly bool $isRecording = true,
        private readonly SpanLimits $limits = new SpanLimits(),
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
     *     resource: array{attributes: array<string, mixed>},
     *     scope: array{name: string, version: string, schemaUrl: null|string, attributes: array<string, mixed>},
     *     attributes: array<string, mixed>,
     *     droppedAttributeCount?: int,
     *     events: array<array{name: string, timestamp: string, attributes: array<string, mixed>, droppedAttributeCount?: int}>,
     *     droppedEventsCount?: int,
     *     links: array<array{context: array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags?: array{byte: int}, traceState?: array{entries: array<string, string>}}, attributes: array<string, mixed>, droppedAttributeCount?: int}>,
     *     droppedLinksCount?: int,
     *     status: null|array{code: int, description: null|string},
     *     isRecording: bool
     * } $data Normalized Span data
     */
    public static function fromArray(array $data): self
    {
        $span = new self(
            $data['name'],
            SpanContext::fromArray($data['context']),
            SpanKind::from($data['kind']),
            new DateTimeImmutable($data['startTime']),
            Resource::fromArray($data['resource']),
            InstrumentationScope::fromArray($data['scope']),
            $data['isRecording'],
        );

        $span->attributes = Attributes::fromArray($data['attributes']);
        $span->droppedAttributeCount = $data['droppedAttributeCount'] ?? 0;

        foreach ($data['events'] as $eventData) {
            $span->events[] = GenericEvent::fromArray($eventData);
        }
        $span->droppedEventsCount = $data['droppedEventsCount'] ?? 0;

        foreach ($data['links'] as $linkData) {
            $span->links[] = SpanLink::fromArray($linkData);
        }
        $span->droppedLinksCount = $data['droppedLinksCount'] ?? 0;

        if ($data['status'] !== null) {
            $span->status = SpanStatus::fromArray($data['status']);
        }

        if ($data['endTime'] !== null) {
            $span->endTime = new DateTimeImmutable($data['endTime']);
        }

        return $span;
    }

    /**
     * Add a link to another span.
     *
     * If the link count limit is exceeded, the link is discarded.
     * Link attributes are enforced against attributePerLinkCountLimit.
     *
     * @return $this
     */
    public function addLink(SpanLink $link): self
    {
        if (count($this->links) >= $this->limits->linkCountLimit) {
            $this->droppedLinksCount++;

            return $this;
        }

        if (
            $link->attributes->count() > $this->limits->attributePerLinkCountLimit
            || $this->limits->attributeValueLengthLimit !== null
        ) {
            $enforcer = new AttributeLimitsEnforcer();
            $result = $enforcer->enforce(
                $link->attributes,
                $this->limits->attributePerLinkCountLimit,
                $this->limits->attributeValueLengthLimit,
            );

            $this->links[] = SpanLink::create($link->context, $result->attributes, $result->droppedAttributeCount);
        } else {
            $this->links[] = $link;
        }

        return $this;
    }

    /**
     * Get all span attributes as array.
     *
     * @return array<string, array<array-key, mixed>|bool|float|int|string>
     */
    public function attributes(): array
    {
        return $this->attributes->normalize();
    }

    /**
     * Get the span attributes as an Attributes object.
     */
    public function attributesObject(): Attributes
    {
        return $this->attributes;
    }

    /**
     * Get the span context.
     */
    public function context(): SpanContext
    {
        return $this->context;
    }

    /**
     * Get the count of attributes that were dropped due to limits.
     */
    public function droppedAttributeCount(): int
    {
        return $this->droppedAttributeCount;
    }

    /**
     * Get the count of events that were dropped due to limits.
     */
    public function droppedEventsCount(): int
    {
        return $this->droppedEventsCount;
    }

    /**
     * Get the count of links that were dropped due to limits.
     */
    public function droppedLinksCount(): int
    {
        return $this->droppedLinksCount;
    }

    /**
     * Get the span duration in milliseconds.
     *
     * Returns null if the span has not ended yet.
     */
    public function duration(): ?float
    {
        if ($this->endTime === null) {
            return null;
        }

        $startMicros = ($this->startTime->getTimestamp() * 1_000_000) + (int) $this->startTime->format('u');
        $endMicros = ($this->endTime->getTimestamp() * 1_000_000) + (int) $this->endTime->format('u');

        return ($endMicros - $startMicros) / 1000;
    }

    /**
     * End the span, optionally with a specific end time.
     *
     * If no end time is provided, the current time is used.
     *
     * @return $this
     */
    public function end(?DateTimeImmutable $endTime = null): self
    {
        if ($this->endTime === null) {
            $this->endTime = $endTime ?? new DateTimeImmutable();
        }

        return $this;
    }

    /**
     * Get the span end time, if ended.
     */
    public function endTime(): ?DateTimeImmutable
    {
        return $this->endTime;
    }

    /**
     * Get all recorded events.
     *
     * @return array<SpanEvent>
     */
    public function events(): array
    {
        return $this->events;
    }

    /**
     * Check if the span has ended.
     */
    public function isEnded(): bool
    {
        return $this->endTime !== null;
    }

    /**
     * Whether the span was already handed to the processor. Distinct from isEnded(): callers may end a span to
     * read its duration and complete it later.
     */
    public function isCompleted(): bool
    {
        return $this->completed;
    }

    /**
     * @internal used by Tracer::complete() to keep completion idempotent
     */
    public function markCompleted(): self
    {
        $this->completed = true;

        return $this;
    }

    /**
     * Check if this span is recording events, attributes, and links.
     *
     * Non-recording spans are created when sampling decisions indicate
     * the span should not be exported. Non-recording spans still propagate
     * context but don't record any data.
     */
    public function isRecording(): bool
    {
        return $this->isRecording;
    }

    /**
     * Get the span kind.
     */
    public function kind(): SpanKind
    {
        return $this->kind;
    }

    /**
     * Get all span links.
     *
     * @return array<SpanLink>
     */
    public function links(): array
    {
        return $this->links;
    }

    /**
     * Get the span name.
     */
    public function name(): string
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
     *     resource: array{attributes: array<string, mixed>},
     *     scope: array{name: string, version: string, schemaUrl: null|string, attributes: array<string, mixed>},
     *     attributes: array<string, mixed>,
     *     droppedAttributeCount: int,
     *     events: array<array{name: string, timestamp: string, attributes: array<string, mixed>, droppedAttributeCount: int}>,
     *     droppedEventsCount: int,
     *     links: array<array{context: array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags: array{byte: int}, traceState: array{entries: array<string, string>}}, attributes: array<string, mixed>, droppedAttributeCount: int}>,
     *     droppedLinksCount: int,
     *     status: null|array{code: int, description: null|string},
     *     isRecording: bool
     * }
     */
    public function normalize(): array
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
            'droppedAttributeCount' => $this->droppedAttributeCount,
            'events' => $events,
            'droppedEventsCount' => $this->droppedEventsCount,
            'links' => $links,
            'droppedLinksCount' => $this->droppedLinksCount,
            'status' => $this->status?->normalize(),
            'isRecording' => $this->isRecording,
        ];
    }

    /**
     * Record an event in this span.
     *
     * If the event count limit is exceeded, the event is discarded.
     * Event attributes are enforced against attributePerEventCountLimit.
     *
     * @return $this
     */
    public function recordEvent(SpanEvent $event): self
    {
        if (count($this->events) >= $this->limits->eventCountLimit) {
            $this->droppedEventsCount++;

            return $this;
        }

        if (
            $event->attributesObject()->count() > $this->limits->attributePerEventCountLimit
            || $this->limits->attributeValueLengthLimit !== null
        ) {
            $enforcer = new AttributeLimitsEnforcer();
            $result = $enforcer->enforce(
                $event->attributesObject(),
                $this->limits->attributePerEventCountLimit,
                $this->limits->attributeValueLengthLimit,
            );

            $this->events[] = GenericEvent::create(
                $event->name(),
                $event->timestamp(),
                $result->attributes,
                $result->droppedAttributeCount,
            );
        } else {
            $this->events[] = $event;
        }

        return $this;
    }

    /**
     * Record an exception as an event.
     *
     * Creates an event with OpenTelemetry semantic conventions for exceptions.
     *
     * @param \Throwable $exception The exception to record
     * @param \DateTimeImmutable $timestamp The timestamp when the exception occurred
     * @param Attributes|TAttributeValueMap $attributes Additional attributes
     *
     * @return $this
     */
    public function recordException(
        Throwable $exception,
        DateTimeImmutable $timestamp,
        Attributes|array $attributes = [],
    ): self {
        $attrs = $attributes instanceof Attributes ? $attributes : Attributes::create($attributes);
        $eventAttributes = Attributes::create([
            'exception.type' => $exception::class,
            'exception.message' => $exception->getMessage(),
            'exception.stacktrace' => $exception->getTraceAsString(),
        ])->merge($attrs);

        $this->events[] = GenericEvent::create('exception', $timestamp, $eventAttributes);

        return $this;
    }

    /**
     * Rename the span.
     *
     * @return $this
     */
    public function rename(string $name): self
    {
        $this->name = $name;

        return $this;
    }

    /**
     * Get the resource describing the entity producing telemetry.
     */
    public function resource(): Resource
    {
        return $this->resource;
    }

    /**
     * Get the instrumentation scope that created this span.
     */
    public function scope(): InstrumentationScope
    {
        return $this->scope;
    }

    /**
     * Set a single attribute.
     *
     * If the attribute count limit is exceeded, the attribute is discarded.
     * If the value is a string exceeding the length limit, it is truncated.
     *
     * @param TAttributeValue $value
     *
     * @return $this
     */
    public function setAttribute(string $key, string|int|float|bool|DateTimeInterface|Throwable|array $value): self
    {
        if (!$this->attributes->has($key) && $this->attributes->count() >= $this->limits->attributeCountLimit) {
            $this->droppedAttributeCount++;

            return $this;
        }

        $value = $this->truncateValue($value);
        $this->attributes = $this->attributes->with($key, $value);

        return $this;
    }

    /**
     * Set multiple attributes at once.
     *
     * Attributes are added until the limit is reached. Excess attributes are discarded.
     *
     * @param Attributes|TAttributeValueMap $attributes
     *
     * @return $this
     */
    public function setAttributes(Attributes|array $attributes): self
    {
        $attrs = $attributes instanceof Attributes ? $attributes : Attributes::create($attributes);
        $enforcer = new AttributeLimitsEnforcer();

        $merged = $this->attributes->merge($attrs);
        $result = $enforcer->enforce(
            $merged,
            $this->limits->attributeCountLimit,
            $this->limits->attributeValueLengthLimit,
        );

        $this->attributes = $result->attributes;
        $this->droppedAttributeCount += $result->droppedAttributeCount;

        return $this;
    }

    /**
     * Set the span status.
     *
     * @return $this
     */
    public function setStatus(SpanStatus $status): self
    {
        $this->status = $status;

        return $this;
    }

    /**
     * Get the span start time.
     */
    public function startTime(): DateTimeImmutable
    {
        return $this->startTime;
    }

    /**
     * Get the span status.
     */
    public function status(): ?SpanStatus
    {
        return $this->status;
    }

    /**
     * Truncate a value according to attribute value length limit.
     *
     * @param TAttributeValue $value
     *
     * @return TAttributeValue
     */
    private function truncateValue(string|int|float|bool|DateTimeInterface|Throwable|array $value): string|int|float|bool|DateTimeInterface|Throwable|array
    {
        $limit = $this->limits->attributeValueLengthLimit;

        if ($limit === null) {
            return $value;
        }

        if (is_string($value) && mb_strlen($value) > $limit) {
            return mb_substr($value, 0, $limit);
        }

        if (is_array($value)) {
            return array_map(function (mixed $item) use ($limit): mixed {
                if (is_string($item) && mb_strlen($item) > $limit) {
                    return mb_substr($item, 0, $limit);
                }

                if (is_array($item)) {
                    return $this->truncateValue($item);
                }

                return $item;
            }, $value);
        }

        return $value;
    }
}
