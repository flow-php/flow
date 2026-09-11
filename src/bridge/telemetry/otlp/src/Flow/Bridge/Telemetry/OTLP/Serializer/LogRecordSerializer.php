<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Serializer;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Tracer\SpanContext;

/**
 * Serializes log records to OTLP JSON format.
 *
 * Converts Flow Telemetry log record arrays to the OTLP JSON wire format.
 *
 * @import-type TAttributeValue from Attributes
 *
 * @see https://opentelemetry.io/docs/specs/otlp/#otlphttp-request
 */
final readonly class LogRecordSerializer
{
    public function __construct(
        private AttributeSerializer $attributeSerializer = new AttributeSerializer(),
    ) {}

    /**
     * Serialize a log record to OTLP format.
     *
     * @param array{
     *     severity: Severity,
     *     body: string,
     *     attributes: array<string, null|TAttributeValue>,
     *     timestamp: float,
     *     spanContext: null|SpanContext
     * } $record
     *
     * @return array<string, mixed>
     */
    public function serialize(array $record): array
    {
        $result = [
            'timeUnixNano' => $this->floatToNanoseconds($record['timestamp']),
            'observedTimeUnixNano' => $this->floatToNanoseconds($record['timestamp']),
            'severityNumber' => $record['severity']->value,
            'severityText' => $record['severity']->name(),
            'body' => ['stringValue' => $record['body']],
            'attributes' => $this->attributeSerializer->serialize(Attributes::create($record['attributes'])),
        ];

        $spanContext = $record['spanContext'];

        if ($spanContext !== null) {
            $result['traceId'] = $spanContext->traceId->toHex();
            $result['spanId'] = $spanContext->spanId->toHex();

            if ($spanContext->traceFlags->isSampled()) {
                $result['flags'] = 1;
            }
        }

        return $result;
    }

    /**
     * Convert timestamp float to nanoseconds string.
     */
    private function floatToNanoseconds(float $timestamp): string
    {
        $seconds = (int) $timestamp;
        $fraction = $timestamp - $seconds;
        $nanoseconds = ($seconds * 1_000_000_000) + (int) ($fraction * 1_000_000_000);

        return (string) $nanoseconds;
    }
}
