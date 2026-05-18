<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Serializer;

use DateTimeImmutable;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Meter\Exemplar;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanEvent;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanLink;
use Flow\Telemetry\Tracer\SpanStatusCode;

use function array_filter;
use function array_map;
use function array_merge;
use function count;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;
use function json_encode;
use function ksort;
use function md5;
use function str_starts_with;

use const ARRAY_FILTER_USE_KEY;
use const JSON_THROW_ON_ERROR;
use const JSON_UNESCAPED_SLASHES;

/**
 * JSON serializer for OTLP wire format.
 *
 * Converts Flow Telemetry objects to the OTLP JSON wire format.
 *
 * @see https://opentelemetry.io/docs/specs/otlp/#otlphttp-request
 */
final class JsonSerializer
{
    /**
     * @param array<LogEntry> $entries
     */
    public function serializeLogs(array $entries): string
    {
        $resourceLogs = [];

        foreach ($this->groupLogsByResource($entries) as $resourceGroup) {
            $scopeLogs = [];

            foreach ($this->groupLogsByScope($resourceGroup['entries']) as $group) {
                $records = [];

                foreach ($group['entries'] as $entry) {
                    $records[] = $this->serializeLogRecord($entry);
                }

                $scopeLogs[] = [
                    'scope' => $this->serializeScope($group['scope']),
                    'logRecords' => $records,
                ];
            }

            $resourceLogs[] = [
                'resource' => $this->serializeResource($resourceGroup['resource']),
                'scopeLogs' => $scopeLogs,
            ];
        }

        $payload = [
            'resourceLogs' => $resourceLogs,
        ];

        return json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    }

    /**
     * @param array<Metric> $metrics
     */
    public function serializeMetrics(array $metrics): string
    {
        $resourceMetrics = [];

        foreach ($this->groupMetricsByResource($metrics) as $resourceGroup) {
            $scopeMetrics = [];

            foreach ($this->groupMetricsByScope($resourceGroup['metrics']) as $group) {
                $serializedMetrics = [];

                foreach ($group['metrics'] as $metric) {
                    $serializedMetrics[] = $this->serializeMetric($metric);
                }

                $scopeMetrics[] = [
                    'scope' => $this->serializeScope($group['scope']),
                    'metrics' => $serializedMetrics,
                ];
            }

            $resourceMetrics[] = [
                'resource' => $this->serializeResource($resourceGroup['resource']),
                'scopeMetrics' => $scopeMetrics,
            ];
        }

        $payload = [
            'resourceMetrics' => $resourceMetrics,
        ];

        return json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    }

    /**
     * @param array<Span> $spans
     */
    public function serializeSpans(array $spans): string
    {
        $resourceSpans = [];

        foreach ($this->groupSpansByResource($spans) as $resourceGroup) {
            $scopeSpans = [];

            foreach ($this->groupSpansByScope($resourceGroup['spans']) as $group) {
                $serializedSpans = [];

                foreach ($group['spans'] as $span) {
                    $serializedSpans[] = $this->serializeSpan($span);
                }

                $scopeSpans[] = [
                    'scope' => $this->serializeScope($group['scope']),
                    'spans' => $serializedSpans,
                ];
            }

            $resourceSpans[] = [
                'resource' => $this->serializeResource($resourceGroup['resource']),
                'scopeSpans' => $scopeSpans,
            ];
        }

        $payload = [
            'resourceSpans' => $resourceSpans,
        ];

        return json_encode($payload, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    }

    /**
     * Create a histogram data point.
     *
     * @return array<string, mixed>
     */
    private function createHistogramDataPoint(Metric $metric): array
    {
        $timestamp = $this->toNanoseconds($metric->timestamp);

        $attributesArray = $metric->attributes->normalize();

        /** @var int $count */
        $count = $attributesArray['histogram.count'] ?? 1;

        /** @var float|int $sum */
        $sum = $attributesArray['histogram.sum'] ?? $metric->value;

        /** @var null|float $min */
        $min = $attributesArray['histogram.min'] ?? null;

        /** @var null|float $max */
        $max = $attributesArray['histogram.max'] ?? null;

        /** @var array<int> $bucketCounts */
        $bucketCounts = $attributesArray['histogram.bucketCounts'] ?? [];

        /** @var array<float> $explicitBounds */
        $explicitBounds = $attributesArray['histogram.explicitBounds'] ?? [];

        $userAttributes = array_filter(
            $attributesArray,
            static fn(string $key): bool => !str_starts_with($key, 'histogram.'),
            ARRAY_FILTER_USE_KEY,
        );

        $dataPoint = [
            'startTimeUnixNano' => $timestamp,
            'timeUnixNano' => $timestamp,
            'attributes' => $this->serializeAttributes($userAttributes),
            'count' => (string) $count,
            'sum' => is_int($sum) ? (float) $sum : $sum,
            'bucketCounts' => array_map(static fn(int $c): string => (string) $c, $bucketCounts),
            'explicitBounds' => array_map(static fn(int|float $b): float => (float) $b, $explicitBounds),
        ];

        if ($min !== null) {
            $dataPoint['min'] = is_int($min) ? (float) $min : $min;
        }

        if ($max !== null) {
            $dataPoint['max'] = is_int($max) ? (float) $max : $max;
        }

        if (count($metric->exemplars) > 0) {
            $dataPoint['exemplars'] = array_map(fn(Exemplar $e): array => $this->serializeExemplar(
                $e,
            ), $metric->exemplars);
        }

        return $dataPoint;
    }

    /**
     * Create a data point for sum/gauge metrics.
     *
     * @return array<string, mixed>
     */
    private function createMetricDataPoint(Metric $metric): array
    {
        $timestamp = $this->toNanoseconds($metric->timestamp);
        $startTimestamp = $metric->startTimestamp !== null ? $this->toNanoseconds($metric->startTimestamp) : $timestamp;

        $dataPoint = [
            'startTimeUnixNano' => $startTimestamp,
            'timeUnixNano' => $timestamp,
            'attributes' => $this->serializeAttributes($metric->attributes->normalize()),
        ];

        if (is_int($metric->value)) {
            $dataPoint['asInt'] = (string) $metric->value;
        } else {
            $dataPoint['asDouble'] = $metric->value;
        }

        if (count($metric->exemplars) > 0) {
            $dataPoint['exemplars'] = array_map(fn(Exemplar $e): array => $this->serializeExemplar(
                $e,
            ), $metric->exemplars);
        }

        return $dataPoint;
    }

    /**
     * Group log entries by resource for OTLP format.
     *
     * @param array<LogEntry> $entries
     *
     * @return array<string, array{resource: resource, entries: array<LogEntry>}>
     */
    private function groupLogsByResource(array $entries): array
    {
        $grouped = [];

        foreach ($entries as $entry) {
            $key = $this->resourceKey($entry->resource);

            if (!isset($grouped[$key])) {
                $grouped[$key] = [
                    'resource' => $entry->resource,
                    'entries' => [],
                ];
            }

            $grouped[$key]['entries'][] = $entry;
        }

        return $grouped;
    }

    /**
     * Group log entries by instrumentation scope for OTLP format.
     *
     * @param array<LogEntry> $entries
     *
     * @return array<string, array{scope: InstrumentationScope, entries: array<LogEntry>}>
     */
    private function groupLogsByScope(array $entries): array
    {
        $grouped = [];

        foreach ($entries as $entry) {
            $scope = $entry->scope;
            $key = $scope->name . '@' . $scope->version . '@' . $scope->attributes->id();

            if (!isset($grouped[$key])) {
                $grouped[$key] = [
                    'scope' => $scope,
                    'entries' => [],
                ];
            }

            $grouped[$key]['entries'][] = $entry;
        }

        return $grouped;
    }

    /**
     * Group metrics by resource for OTLP format.
     *
     * @param array<Metric> $metrics
     *
     * @return array<string, array{resource: resource, metrics: array<Metric>}>
     */
    private function groupMetricsByResource(array $metrics): array
    {
        $grouped = [];

        foreach ($metrics as $metric) {
            $key = $this->resourceKey($metric->resource);

            if (!isset($grouped[$key])) {
                $grouped[$key] = [
                    'resource' => $metric->resource,
                    'metrics' => [],
                ];
            }

            $grouped[$key]['metrics'][] = $metric;
        }

        return $grouped;
    }

    /**
     * Group metrics by instrumentation scope for OTLP format.
     *
     * @param array<Metric> $metrics
     *
     * @return array<string, array{scope: InstrumentationScope, metrics: array<Metric>}>
     */
    private function groupMetricsByScope(array $metrics): array
    {
        $grouped = [];

        foreach ($metrics as $metric) {
            $scope = $metric->scope;
            $key = $scope->name . '@' . $scope->version . '@' . $scope->attributes->id();

            if (!isset($grouped[$key])) {
                $grouped[$key] = [
                    'scope' => $scope,
                    'metrics' => [],
                ];
            }

            $grouped[$key]['metrics'][] = $metric;
        }

        return $grouped;
    }

    /**
     * Group spans by resource for OTLP format.
     *
     * @param array<Span> $spans
     *
     * @return array<string, array{resource: resource, spans: array<Span>}>
     */
    private function groupSpansByResource(array $spans): array
    {
        $grouped = [];

        foreach ($spans as $span) {
            $key = $this->resourceKey($span->resource());

            if (!isset($grouped[$key])) {
                $grouped[$key] = [
                    'resource' => $span->resource(),
                    'spans' => [],
                ];
            }

            $grouped[$key]['spans'][] = $span;
        }

        return $grouped;
    }

    /**
     * Group spans by instrumentation scope for OTLP format.
     *
     * @param array<Span> $spans
     *
     * @return array<string, array{scope: InstrumentationScope, spans: array<Span>}>
     */
    private function groupSpansByScope(array $spans): array
    {
        $grouped = [];

        foreach ($spans as $span) {
            $scope = $span->scope();
            $key = $scope->name . '@' . $scope->version . '@' . $scope->attributes->id();

            if (!isset($grouped[$key])) {
                $grouped[$key] = [
                    'scope' => $scope,
                    'spans' => [],
                ];
            }

            $grouped[$key]['spans'][] = $span;
        }

        return $grouped;
    }

    /**
     * Generate a unique key for a Resource based on its attributes.
     */
    private function resourceKey(Resource $resource): string
    {
        $attributes = $resource->all();
        ksort($attributes);

        return md5(json_encode($attributes, JSON_THROW_ON_ERROR));
    }

    /**
     * Serialize attributes array to OTLP format.
     *
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes
     *
     * @return array<array{key: string, value: array<string, mixed>}>
     */
    private function serializeAttributes(array $attributes): array
    {
        $result = [];

        foreach ($attributes as $key => $value) {
            $result[] = [
                'key' => $key,
                'value' => $this->serializeAttributeValue($value),
            ];
        }

        return $result;
    }

    /**
     * Serialize a single attribute value to OTLP format.
     *
     * @param array<bool|float|int|string>|bool|float|int|string $value
     *
     * @return array<string, mixed>
     */
    private function serializeAttributeValue(string|int|float|bool|array $value): array
    {
        if (is_string($value)) {
            return ['stringValue' => $value];
        }

        if (is_int($value)) {
            return ['intValue' => (string) $value];
        }

        if (is_float($value)) {
            return ['doubleValue' => $value];
        }

        if (is_bool($value)) {
            return ['boolValue' => $value];
        }

        $serialized = [];

        foreach ($value as $v) {
            $serialized[] = $this->serializeAttributeValue($v);
        }

        return [
            'arrayValue' => [
                'values' => $serialized,
            ],
        ];
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
                'attributes' => $this->serializeAttributes($event->attributes()),
            ];
        }

        return $result;
    }

    /**
     * Serialize an Exemplar to OTLP format.
     *
     * @return array<string, mixed>
     */
    private function serializeExemplar(Exemplar $exemplar): array
    {
        $result = [
            'timeUnixNano' => $this->toNanoseconds($exemplar->timestamp),
            'filteredAttributes' => $this->serializeAttributes($exemplar->filteredAttributes),
        ];

        if ($exemplar->traceId->isValid()) {
            $result['traceId'] = $exemplar->traceId->toHex();
        }

        if ($exemplar->spanId->isValid()) {
            $result['spanId'] = $exemplar->spanId->toHex();
        }

        if (is_int($exemplar->value)) {
            $result['asInt'] = (string) $exemplar->value;
        } else {
            $result['asDouble'] = $exemplar->value;
        }

        return $result;
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
            if (!$link->context->isValid()) {
                continue;
            }

            $result[] = [
                'traceId' => $link->context->traceId->toHex(),
                'spanId' => $link->context->spanId->toHex(),
                'attributes' => $this->serializeAttributes($link->attributes->normalize()),
            ];
        }

        return $result;
    }

    /**
     * Serialize a LogEntry to OTLP format.
     *
     * @return array<string, mixed>
     */
    private function serializeLogRecord(LogEntry $entry): array
    {
        $result = [
            'timeUnixNano' => $this->toNanoseconds($entry->timestamp),
            'observedTimeUnixNano' => $this->toNanoseconds($entry->timestamp),
            'severityNumber' => $entry->record->severity->value,
            'severityText' => $entry->record->severity->name(),
            'body' => ['stringValue' => $entry->record->body],
            'attributes' => $this->serializeAttributes($entry->record->attributes->normalize()),
        ];

        if ($entry->spanContext !== null && $entry->spanContext->isValid()) {
            $result['traceId'] = $entry->spanContext->traceId->toHex();
            $result['spanId'] = $entry->spanContext->spanId->toHex();

            if ($entry->spanContext->traceFlags->isSampled()) {
                $result['flags'] = 1;
            }
        }

        return $result;
    }

    /**
     * Serialize a Metric to OTLP format.
     *
     * @return array<string, mixed>
     */
    private function serializeMetric(Metric $metric): array
    {
        $result = [
            'name' => $metric->name,
        ];

        if ($metric->description !== null) {
            $result['description'] = $metric->description;
        }

        if ($metric->unit !== null) {
            $result['unit'] = $metric->unit;
        }

        $dataPoint = $this->createMetricDataPoint($metric);

        return match ($metric->type) {
            MetricType::COUNTER => array_merge($result, [
                'sum' => [
                    'dataPoints' => [$dataPoint],
                    'aggregationTemporality' => $metric->temporality->value,
                    'isMonotonic' => true,
                ],
            ]),
            MetricType::UP_DOWN_COUNTER => array_merge($result, [
                'sum' => [
                    'dataPoints' => [$dataPoint],
                    'aggregationTemporality' => $metric->temporality->value,
                    'isMonotonic' => false,
                ],
            ]),
            MetricType::GAUGE => array_merge($result, [
                'gauge' => [
                    'dataPoints' => [$dataPoint],
                ],
            ]),
            MetricType::HISTOGRAM => array_merge($result, [
                'histogram' => [
                    'dataPoints' => [$this->createHistogramDataPoint($metric)],
                    'aggregationTemporality' => $metric->temporality->value,
                ],
            ]),
        };
    }

    /**
     * Serialize a Resource to OTLP format.
     *
     * @return array{attributes: array<array{key: string, value: array<string, mixed>}>}
     */
    private function serializeResource(Resource $resource): array
    {
        return [
            'attributes' => $this->serializeAttributes($resource->all()),
        ];
    }

    /**
     * Serialize an InstrumentationScope to OTLP format.
     *
     * @return array{name: string, version?: string, attributes?: array<array{key: string, value: array<string, mixed>}>}
     */
    private function serializeScope(InstrumentationScope $scope): array
    {
        $result = [
            'name' => $scope->name,
        ];

        if ($scope->version !== 'unknown') {
            $result['version'] = $scope->version;
        }

        if ($scope->attributes->count() > 0) {
            $result['attributes'] = $this->serializeAttributes($scope->attributes->normalize());
        }

        return $result;
    }

    /**
     * Serialize a Span to OTLP format.
     *
     * @return array<string, mixed>
     */
    private function serializeSpan(Span $span): array
    {
        $context = $span->context();

        $result = [
            'traceId' => $context->traceId->toHex(),
            'spanId' => $context->spanId->toHex(),
            'name' => $span->name(),
            'kind' => $this->serializeSpanKind($span->kind()),
            'startTimeUnixNano' => $this->toNanoseconds($span->startTime()),
            'attributes' => $this->serializeAttributes($span->attributes()),
            'events' => $this->serializeEvents($span->events()),
            'links' => $this->serializeLinks($span->links()),
            'status' => $this->serializeSpanStatus($span),
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
     * Convert SpanKind to OTLP span kind value.
     *
     * OTLP span kinds:
     * 0 = SPAN_KIND_UNSPECIFIED
     * 1 = SPAN_KIND_INTERNAL
     * 2 = SPAN_KIND_SERVER
     * 3 = SPAN_KIND_CLIENT
     * 4 = SPAN_KIND_PRODUCER
     * 5 = SPAN_KIND_CONSUMER
     */
    private function serializeSpanKind(SpanKind $kind): int
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
     * Serialize span status to OTLP format from Span.
     *
     * @return array{code: int, message?: string}
     */
    private function serializeSpanStatus(Span $span): array
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
