<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Serializer\Json;

use DateTimeImmutable;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanEvent;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanLink;
use Flow\Telemetry\Tracer\SpanStatusCode;

/**
 * @see https://opentelemetry.io/docs/specs/otlp/#otlphttp-request
 */
final readonly class SpanSerializer
{
    public function __construct(
        private AttributeSerializer $attributeSerializer = new AttributeSerializer(),
    ) {}

    /**
     * Serialize a Span to OTLP format.
     *
     * @return array<string, mixed>
     */
    public function serialize(Span $span): array
    {
        $context = $span->context();

        $result = [
            'traceId' => $context->traceId->toHex(),
            'spanId' => $context->spanId->toHex(),
            'name' => $span->name(),
            'kind' => $this->serializeKind($span->kind()),
            'startTimeUnixNano' => $this->toNanoseconds($span->startTime()),
            'attributes' => $this->attributeSerializer->serialize($span->attributesObject()),
            'events' => $this->serializeEvents($span->events()),
            'links' => $this->serializeLinks($span->links()),
            'status' => $this->serializeStatus($span),
        ];

        if ($context->parentSpanId !== null) {
            $result['parentSpanId'] = $context->parentSpanId->toHex();
        }

        $endTime = $span->endTime();

        if ($endTime !== null) {
            $result['endTimeUnixNano'] = $this->toNanoseconds($endTime);
        }

        if ($context->traceFlags->isSampled()) {
            $result['flags'] = 1;
        }

        return $result;
    }

    /**
     * Serialize span events to OTLP format.
     *
     * @param array<SpanEvent> $events
     *
     * @return array<array{name: string, timeUnixNano: string, attributes: array<array{key: string, value: array<string, mixed>}>}>
     */
    private function serializeEvents(array $events): array
    {
        $result = [];

        foreach ($events as $event) {
            $result[] = [
                'name' => $event->name(),
                'timeUnixNano' => $this->toNanoseconds($event->timestamp()),
                'attributes' => $this->attributeSerializer->serialize($event->attributesObject()),
            ];
        }

        return $result;
    }

    private function serializeKind(SpanKind $kind): int
    {
        return match ($kind) {
            SpanKind::INTERNAL => 1,
            SpanKind::SERVER => 2,
            SpanKind::CLIENT => 3,
            SpanKind::PRODUCER => 4,
            SpanKind::CONSUMER => 5,
        };
    }

    /**
     * Serialize span links to OTLP format.
     *
     * @param array<SpanLink> $links
     *
     * @return array<array{traceId: string, spanId: string, attributes: array<array{key: string, value: array<string, mixed>}>}>
     */
    private function serializeLinks(array $links): array
    {
        $result = [];

        foreach ($links as $link) {
            $result[] = [
                'traceId' => $link->context->traceId->toHex(),
                'spanId' => $link->context->spanId->toHex(),
                'attributes' => $this->attributeSerializer->serialize($link->attributes),
            ];
        }

        return $result;
    }

    /**
     * @return array{code: int, message?: string}
     */
    private function serializeStatus(Span $span): array
    {
        $status = $span->status();

        if ($status === null) {
            return ['code' => 0];
        }

        $result = [
            'code' => match ($status->code) {
                SpanStatusCode::UNSET => 0,
                SpanStatusCode::OK => 1,
                SpanStatusCode::ERROR => 2,
            },
        ];

        if ($status->description !== null) {
            $result['message'] = $status->description;
        }

        return $result;
    }

    /**
     * Convert DateTimeImmutable to nanoseconds since Unix epoch as string.
     */
    private function toNanoseconds(DateTimeImmutable $dateTime): string
    {
        $seconds = (int) $dateTime->format('U');
        $microseconds = (int) $dateTime->format('u');
        $nanoseconds = ($seconds * 1_000_000_000) + ($microseconds * 1_000);

        return (string) $nanoseconds;
    }
}
