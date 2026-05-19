<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

use DateTimeImmutable;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tracer\SpanContext;

/**
 * Complete log entry ready for processing.
 *
 * LogEntry wraps the user-provided LogRecord together with
 * contextual information resolved at emit time:
 * - InstrumentationScope from the Logger
 * - SpanContext from the context storage (for trace correlation)
 * - Resolved timestamp
 *
 * This is the internal type passed to LogProcessor and Exporter implementations.
 */
final readonly class LogEntry
{
    public function __construct(
        public LogRecord $record,
        public Resource $resource,
        public InstrumentationScope $scope,
        public DateTimeImmutable $timestamp,
        public ?SpanContext $spanContext = null,
        public int $droppedAttributeCount = 0,
    ) {}

    /**
     * Create a LogEntry from a normalized array representation.
     *
     * @param array{
     *     record: array{severity: int, body: string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>, timestamp: null|string, observedTimestamp: null|string},
     *     resource: array{attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     scope: array{name: string, version: string, schemaUrl: null|string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     timestamp: string,
     *     spanContext: null|array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags: array{byte: int}, traceState: array{entries: array<string, string>}},
     *     droppedAttributeCount?: int
     * } $data Normalized LogEntry data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            LogRecord::fromArray($data['record']),
            Resource::fromArray($data['resource']),
            InstrumentationScope::fromArray($data['scope']),
            new DateTimeImmutable($data['timestamp']),
            $data['spanContext'] !== null ? SpanContext::fromArray($data['spanContext']) : null,
            $data['droppedAttributeCount'] ?? 0,
        );
    }

    /**
     * Normalize the LogEntry to an array representation for serialization.
     *
     * @return array{
     *     record: array{severity: int, body: string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>, timestamp: null|string, observedTimestamp: null|string},
     *     resource: array{attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     scope: array{name: string, version: string, schemaUrl: null|string, attributes: array<string, array<bool|float|int|string>|bool|float|int|string>},
     *     timestamp: string,
     *     spanContext: null|array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags: array{byte: int}, traceState: array{entries: array<string, string>}},
     *     droppedAttributeCount: int
     * }
     */
    public function normalize(): array
    {
        return [
            'record' => $this->record->normalize(),
            'resource' => $this->resource->normalize(),
            'scope' => $this->scope->normalize(),
            'timestamp' => $this->timestamp->format('c'),
            'spanContext' => $this->spanContext?->normalize(),
            'droppedAttributeCount' => $this->droppedAttributeCount,
        ];
    }
}
