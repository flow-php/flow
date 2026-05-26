<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Serializer;

use DateTimeImmutable;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\Exemplar;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanEvent;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanLink;
use Flow\Telemetry\Tracer\SpanStatusCode;
use Opentelemetry\Proto\Collector\Logs\V1\ExportLogsServiceRequest;
use Opentelemetry\Proto\Collector\Metrics\V1\ExportMetricsServiceRequest;
use Opentelemetry\Proto\Collector\Trace\V1\ExportTraceServiceRequest;
use Opentelemetry\Proto\Common\V1\AnyValue;
use Opentelemetry\Proto\Common\V1\ArrayValue;
use Opentelemetry\Proto\Common\V1\InstrumentationScope as ProtoInstrumentationScope;
use Opentelemetry\Proto\Common\V1\KeyValue;
use Opentelemetry\Proto\Common\V1\KeyValueList;
use Opentelemetry\Proto\Logs\V1\LogRecord;
use Opentelemetry\Proto\Logs\V1\ResourceLogs;
use Opentelemetry\Proto\Logs\V1\ScopeLogs;
use Opentelemetry\Proto\Logs\V1\SeverityNumber;
use Opentelemetry\Proto\Metrics\V1\AggregationTemporality;
use Opentelemetry\Proto\Metrics\V1\Exemplar as ProtoExemplar;
use Opentelemetry\Proto\Metrics\V1\Gauge;
use Opentelemetry\Proto\Metrics\V1\Histogram;
use Opentelemetry\Proto\Metrics\V1\HistogramDataPoint;
use Opentelemetry\Proto\Metrics\V1\Metric as ProtoMetric;
use Opentelemetry\Proto\Metrics\V1\NumberDataPoint;
use Opentelemetry\Proto\Metrics\V1\ResourceMetrics;
use Opentelemetry\Proto\Metrics\V1\ScopeMetrics;
use Opentelemetry\Proto\Metrics\V1\Sum;
use Opentelemetry\Proto\Resource\V1\Resource as ProtoResource;
use Opentelemetry\Proto\Trace\V1\ResourceSpans;
use Opentelemetry\Proto\Trace\V1\ScopeSpans;
use Opentelemetry\Proto\Trace\V1\Span as ProtoSpan;
use Opentelemetry\Proto\Trace\V1\Span\Event;
use Opentelemetry\Proto\Trace\V1\Span\Link;
use Opentelemetry\Proto\Trace\V1\Span\SpanKind as ProtoSpanKind;
use Opentelemetry\Proto\Trace\V1\Status;
use Opentelemetry\Proto\Trace\V1\Status\StatusCode;
use RuntimeException;

use function array_filter;
use function array_is_list;
use function array_keys;
use function array_map;
use function class_exists;
use function count;
use function get_debug_type;
use function hex2bin;
use function is_array;
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

/**
 * Serializes Flow Telemetry objects to OTLP Protobuf wire format.
 *
 * @see https://opentelemetry.io/docs/specs/otlp/
 */
final class ProtobufSerializer implements GrpcRequestFactory
{
    public function __construct()
    {
        if (!class_exists('Google\Protobuf\Internal\Message')) {
            throw new RuntimeException('The google/protobuf package is required for ProtobufSerializer. '
            . 'Install it via: composer require google/protobuf');
        }
    }

    /**
     * Create an ExportLogsServiceRequest for gRPC transport.
     *
     * @param array<LogEntry> $entries
     */
    public function createLogsRequest(array $entries): ExportLogsServiceRequest
    {
        $request = new ExportLogsServiceRequest();
        $resourceLogsList = [];

        foreach ($this->groupLogsByResource($entries) as $resourceGroup) {
            $resourceLogs = new ResourceLogs();
            $resourceLogs->setResource($this->createProtoResource($resourceGroup['resource']));

            $scopeLogsList = [];

            foreach ($this->groupLogsByScope($resourceGroup['entries']) as $scopeGroup) {
                $scopeLogs = new ScopeLogs();
                $scopeLogs->setScope($this->createProtoScope($scopeGroup['scope']));

                $logRecords = [];

                foreach ($scopeGroup['entries'] as $entry) {
                    $logRecords[] = $this->createLogRecord($entry);
                }

                $scopeLogs->setLogRecords($logRecords);
                $scopeLogsList[] = $scopeLogs;
            }

            $resourceLogs->setScopeLogs($scopeLogsList);
            $resourceLogsList[] = $resourceLogs;
        }

        $request->setResourceLogs($resourceLogsList);

        return $request;
    }

    /**
     * Create an ExportMetricsServiceRequest for gRPC transport.
     *
     * @param array<Metric> $metrics
     */
    public function createMetricsRequest(array $metrics): ExportMetricsServiceRequest
    {
        $request = new ExportMetricsServiceRequest();
        $resourceMetricsList = [];

        foreach ($this->groupMetricsByResource($metrics) as $resourceGroup) {
            $resourceMetrics = new ResourceMetrics();
            $resourceMetrics->setResource($this->createProtoResource($resourceGroup['resource']));

            $scopeMetricsList = [];

            foreach ($this->groupMetricsByScope($resourceGroup['metrics']) as $scopeGroup) {
                $scopeMetrics = new ScopeMetrics();
                $scopeMetrics->setScope($this->createProtoScope($scopeGroup['scope']));

                $protoMetrics = [];

                foreach ($scopeGroup['metrics'] as $metric) {
                    $protoMetrics[] = $this->createProtoMetric($metric);
                }

                $scopeMetrics->setMetrics($protoMetrics);
                $scopeMetricsList[] = $scopeMetrics;
            }

            $resourceMetrics->setScopeMetrics($scopeMetricsList);
            $resourceMetricsList[] = $resourceMetrics;
        }

        $request->setResourceMetrics($resourceMetricsList);

        return $request;
    }

    /**
     * Create an ExportTraceServiceRequest for gRPC transport.
     *
     * @param array<Span> $spans
     */
    public function createSpansRequest(array $spans): ExportTraceServiceRequest
    {
        $request = new ExportTraceServiceRequest();
        $resourceSpansList = [];

        foreach ($this->groupSpansByResource($spans) as $resourceGroup) {
            $resourceSpans = new ResourceSpans();
            $resourceSpans->setResource($this->createProtoResource($resourceGroup['resource']));

            $scopeSpansList = [];

            foreach ($this->groupSpansByScope($resourceGroup['spans']) as $scopeGroup) {
                $scopeSpans = new ScopeSpans();
                $scopeSpans->setScope($this->createProtoScope($scopeGroup['scope']));

                $protoSpans = [];

                foreach ($scopeGroup['spans'] as $span) {
                    $protoSpans[] = $this->createProtoSpan($span);
                }

                $scopeSpans->setSpans($protoSpans);
                $scopeSpansList[] = $scopeSpans;
            }

            $resourceSpans->setScopeSpans($scopeSpansList);
            $resourceSpansList[] = $resourceSpans;
        }

        $request->setResourceSpans($resourceSpansList);

        return $request;
    }

    /**
     * @param array<LogEntry> $entries
     */
    public function serializeLogs(array $entries): string
    {
        return $this->createLogsRequest($entries)->serializeToString();
    }

    /**
     * @param array<Metric> $metrics
     */
    public function serializeMetrics(array $metrics): string
    {
        return $this->createMetricsRequest($metrics)->serializeToString();
    }

    /**
     * @param array<Span> $spans
     */
    public function serializeSpans(array $spans): string
    {
        return $this->createSpansRequest($spans)->serializeToString();
    }

    private function createAnyValue(mixed $value): AnyValue
    {
        $anyValue = new AnyValue();

        if (is_string($value)) {
            $anyValue->setStringValue($value);
        } elseif (is_int($value)) {
            $anyValue->setIntValue($value);
        } elseif (is_float($value)) {
            $anyValue->setDoubleValue($value);
        } elseif (is_bool($value)) {
            $anyValue->setBoolValue($value);
        } elseif (is_array($value)) {
            if (array_is_list($value)) {
                $arrayValue = new ArrayValue();
                $arrayValue->setValues(array_map($this->createAnyValue(...), $value));
                $anyValue->setArrayValue($arrayValue);
            } else {
                $kvList = new KeyValueList();
                $kvList->setValues(array_map(
                    function (int|string $k, mixed $v): KeyValue {
                        $kv = new KeyValue();
                        $kv->setKey((string) $k);
                        $kv->setValue($this->createAnyValue($v));

                        return $kv;
                    },
                    array_keys($value),
                    $value,
                ));
                $anyValue->setKvlistValue($kvList);
            }
        } else {
            $anyValue->setStringValue(get_debug_type($value));
        }

        return $anyValue;
    }

    private function createHistogramDataPoint(Metric $metric): HistogramDataPoint
    {
        $dataPoint = new HistogramDataPoint();
        $timestamp = $this->toNanoseconds($metric->timestamp);

        $dataPoint->setStartTimeUnixNano($timestamp);
        $dataPoint->setTimeUnixNano($timestamp);

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

        $dataPoint->setAttributes($this->createKeyValues($userAttributes));
        $dataPoint->setCount($count);
        $dataPoint->setSum(is_int($sum) ? (float) $sum : $sum);
        $dataPoint->setBucketCounts($bucketCounts);
        $dataPoint->setExplicitBounds(array_map(static fn(int|float $b): float => (float) $b, $explicitBounds));

        if ($min !== null) {
            $dataPoint->setMin($min);
        }

        if ($max !== null) {
            $dataPoint->setMax($max);
        }

        if (count($metric->exemplars) > 0) {
            $exemplars = [];

            foreach ($metric->exemplars as $exemplar) {
                $exemplars[] = $this->createProtoExemplar($exemplar);
            }

            $dataPoint->setExemplars($exemplars);
        }

        return $dataPoint;
    }

    /**
     * @param array<string, mixed> $attributes
     *
     * @return array<KeyValue>
     */
    private function createKeyValues(array $attributes): array
    {
        return array_map(
            function (string $key, mixed $value): KeyValue {
                $keyValue = new KeyValue();
                $keyValue->setKey($key);
                $keyValue->setValue($this->createAnyValue($value));

                return $keyValue;
            },
            array_keys($attributes),
            $attributes,
        );
    }

    private function createLogRecord(LogEntry $entry): LogRecord
    {
        $logRecord = new LogRecord();

        $timestampNanos = $this->toNanoseconds($entry->timestamp);
        $logRecord->setTimeUnixNano($timestampNanos);
        $logRecord->setObservedTimeUnixNano($timestampNanos);

        $logRecord->setSeverityNumber($this->mapSeverityNumber($entry->record->severity));
        $logRecord->setSeverityText($entry->record->severity->name());

        $body = new AnyValue();
        $body->setStringValue($entry->record->body);
        $logRecord->setBody($body);

        $logRecord->setAttributes($this->createKeyValues($entry->record->attributes->normalize()));

        if ($entry->spanContext !== null && $entry->spanContext->isValid()) {
            $logRecord->setTraceId(hex2bin($entry->spanContext->traceId->toHex()) ?: '');
            $logRecord->setSpanId(hex2bin($entry->spanContext->spanId->toHex()) ?: '');

            if ($entry->spanContext->traceFlags->isSampled()) {
                $logRecord->setFlags(1);
            }
        }

        return $logRecord;
    }

    private function createNumberDataPoint(Metric $metric): NumberDataPoint
    {
        $dataPoint = new NumberDataPoint();
        $timestamp = $this->toNanoseconds($metric->timestamp);

        $dataPoint->setStartTimeUnixNano($timestamp);
        $dataPoint->setTimeUnixNano($timestamp);
        $dataPoint->setAttributes($this->createKeyValues($metric->attributes->normalize()));

        if (is_int($metric->value)) {
            $dataPoint->setAsInt($metric->value);
        } else {
            $dataPoint->setAsDouble($metric->value);
        }

        if (count($metric->exemplars) > 0) {
            $exemplars = [];

            foreach ($metric->exemplars as $exemplar) {
                $exemplars[] = $this->createProtoExemplar($exemplar);
            }

            $dataPoint->setExemplars($exemplars);
        }

        return $dataPoint;
    }

    private function createProtoExemplar(Exemplar $exemplar): ProtoExemplar
    {
        $protoExemplar = new ProtoExemplar();
        $protoExemplar->setTimeUnixNano($this->toNanoseconds($exemplar->timestamp));

        if ($exemplar->traceId->isValid()) {
            $protoExemplar->setTraceId(hex2bin($exemplar->traceId->toHex()) ?: '');
        }

        if ($exemplar->spanId->isValid()) {
            $protoExemplar->setSpanId(hex2bin($exemplar->spanId->toHex()) ?: '');
        }

        $protoExemplar->setFilteredAttributes($this->createKeyValues($exemplar->filteredAttributes));

        if (is_int($exemplar->value)) {
            $protoExemplar->setAsInt($exemplar->value);
        } else {
            $protoExemplar->setAsDouble($exemplar->value);
        }

        return $protoExemplar;
    }

    private function createProtoMetric(Metric $metric): ProtoMetric
    {
        $protoMetric = new ProtoMetric();
        $protoMetric->setName($metric->name);

        if ($metric->description !== null) {
            $protoMetric->setDescription($metric->description);
        }

        if ($metric->unit !== null) {
            $protoMetric->setUnit($metric->unit);
        }

        return match ($metric->type) {
            MetricType::COUNTER => $this->setCounterData($protoMetric, $metric, true),
            MetricType::UP_DOWN_COUNTER => $this->setCounterData($protoMetric, $metric, false),
            MetricType::GAUGE => $this->setGaugeData($protoMetric, $metric),
            MetricType::HISTOGRAM => $this->setHistogramData($protoMetric, $metric),
        };
    }

    private function createProtoResource(Resource $resource): ProtoResource
    {
        $protoResource = new ProtoResource();
        $protoResource->setAttributes($this->createKeyValues($resource->all()));

        return $protoResource;
    }

    private function createProtoScope(InstrumentationScope $scope): ProtoInstrumentationScope
    {
        $protoScope = new ProtoInstrumentationScope();
        $protoScope->setName($scope->name);

        if ($scope->version !== 'unknown') {
            $protoScope->setVersion($scope->version);
        }

        if ($scope->attributes->count() > 0) {
            $protoScope->setAttributes($this->createKeyValues($scope->attributes->normalize()));
        }

        return $protoScope;
    }

    private function createProtoSpan(Span $span): ProtoSpan
    {
        $protoSpan = new ProtoSpan();
        $context = $span->context();

        $protoSpan->setTraceId(hex2bin($context->traceId->toHex()) ?: '');
        $protoSpan->setSpanId(hex2bin($context->spanId->toHex()) ?: '');
        $protoSpan->setName($span->name());
        $protoSpan->setKind($this->mapSpanKind($span->kind()));
        $protoSpan->setStartTimeUnixNano($this->toNanoseconds($span->startTime()));
        $protoSpan->setAttributes($this->createKeyValues($span->attributes()));
        $events = [];

        foreach ($span->events() as $event) {
            $events[] = $this->createSpanEvent($event);
        }

        $protoSpan->setEvents($events);
        $links = [];

        foreach ($span->links() as $link) {
            $protoLink = $this->createSpanLink($link);

            if ($protoLink !== null) {
                $links[] = $protoLink;
            }
        }

        $protoSpan->setLinks($links);
        $protoSpan->setStatus($this->createSpanStatus($span));

        if ($context->parentSpanId !== null) {
            $protoSpan->setParentSpanId(hex2bin($context->parentSpanId->toHex()) ?: '');
        }

        $endTime = $span->endTime();

        if ($endTime !== null) {
            $protoSpan->setEndTimeUnixNano($this->toNanoseconds($endTime));
        }

        if ($context->traceFlags->isSampled()) {
            $protoSpan->setFlags(1);
        }

        return $protoSpan;
    }

    private function createSpanEvent(SpanEvent $event): Event
    {
        $protoEvent = new Event();
        $protoEvent->setName($event->name());
        $protoEvent->setTimeUnixNano($this->toNanoseconds($event->timestamp()));
        $protoEvent->setAttributes($this->createKeyValues($event->attributes()));

        return $protoEvent;
    }

    private function createSpanLink(SpanLink $link): ?Link
    {
        if (!$link->context->isValid()) {
            return null;
        }

        $protoLink = new Link();
        $protoLink->setTraceId(hex2bin($link->context->traceId->toHex()) ?: '');
        $protoLink->setSpanId(hex2bin($link->context->spanId->toHex()) ?: '');
        $protoLink->setAttributes($this->createKeyValues($link->attributes->normalize()));

        return $protoLink;
    }

    private function createSpanStatus(Span $span): Status
    {
        $status = new Status();
        $spanStatus = $span->status();

        if ($spanStatus === null) {
            $status->setCode(StatusCode::STATUS_CODE_UNSET);

            return $status;
        }

        $status->setCode(match ($spanStatus->code) {
            SpanStatusCode::UNSET => StatusCode::STATUS_CODE_UNSET,
            SpanStatusCode::OK => StatusCode::STATUS_CODE_OK,
            SpanStatusCode::ERROR => StatusCode::STATUS_CODE_ERROR,
        });

        if ($spanStatus->description !== null) {
            $status->setMessage($spanStatus->description);
        }

        return $status;
    }

    /**
     * Group log entries by resource for OTLP format.
     *
     * @param array<LogEntry> $entries
     *
     * @return array<string, array{resource: \Flow\Telemetry\Resource, entries: array<LogEntry>}>
     */
    private function groupLogsByResource(array $entries): array
    {
        $grouped = [];

        foreach ($entries as $entry) {
            $key = $this->resourceKey($entry->resource);
            $grouped[$key] ??= [
                'resource' => $entry->resource,
                'entries' => [],
            ];
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
            $key = $scope->name . '@' . $scope->version;
            $grouped[$key] ??= [
                'scope' => $scope,
                'entries' => [],
            ];
            $grouped[$key]['entries'][] = $entry;
        }

        return $grouped;
    }

    /**
     * Group metrics by resource for OTLP format.
     *
     * @param array<Metric> $metrics
     *
     * @return array<string, array{resource: \Flow\Telemetry\Resource, metrics: array<Metric>}>
     */
    private function groupMetricsByResource(array $metrics): array
    {
        $grouped = [];

        foreach ($metrics as $metric) {
            $key = $this->resourceKey($metric->resource);
            $grouped[$key] ??= [
                'resource' => $metric->resource,
                'metrics' => [],
            ];
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
            $key = $scope->name . '@' . $scope->version;
            $grouped[$key] ??= [
                'scope' => $scope,
                'metrics' => [],
            ];
            $grouped[$key]['metrics'][] = $metric;
        }

        return $grouped;
    }

    /**
     * Group spans by resource for OTLP format.
     *
     * @param array<Span> $spans
     *
     * @return array<string, array{resource: \Flow\Telemetry\Resource, spans: array<Span>}>
     */
    private function groupSpansByResource(array $spans): array
    {
        $grouped = [];

        foreach ($spans as $span) {
            $key = $this->resourceKey($span->resource());
            $grouped[$key] ??= [
                'resource' => $span->resource(),
                'spans' => [],
            ];
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
            $key = $scope->name . '@' . $scope->version;
            $grouped[$key] ??= [
                'scope' => $scope,
                'spans' => [],
            ];
            $grouped[$key]['spans'][] = $span;
        }

        return $grouped;
    }

    private function mapSeverityNumber(Severity $severity): int
    {
        return match ($severity) {
            Severity::TRACE => SeverityNumber::SEVERITY_NUMBER_TRACE,
            Severity::DEBUG => SeverityNumber::SEVERITY_NUMBER_DEBUG,
            Severity::INFO => SeverityNumber::SEVERITY_NUMBER_INFO,
            Severity::WARN => SeverityNumber::SEVERITY_NUMBER_WARN,
            Severity::ERROR => SeverityNumber::SEVERITY_NUMBER_ERROR,
            Severity::FATAL => SeverityNumber::SEVERITY_NUMBER_FATAL,
        };
    }

    private function mapSpanKind(SpanKind $kind): int
    {
        return match ($kind) {
            SpanKind::INTERNAL => ProtoSpanKind::SPAN_KIND_INTERNAL,
            SpanKind::SERVER => ProtoSpanKind::SPAN_KIND_SERVER,
            SpanKind::CLIENT => ProtoSpanKind::SPAN_KIND_CLIENT,
            SpanKind::PRODUCER => ProtoSpanKind::SPAN_KIND_PRODUCER,
            SpanKind::CONSUMER => ProtoSpanKind::SPAN_KIND_CONSUMER,
        };
    }

    private function mapTemporality(Metric $metric): int
    {
        return match ($metric->temporality->value) {
            1 => AggregationTemporality::AGGREGATION_TEMPORALITY_DELTA,
            2 => AggregationTemporality::AGGREGATION_TEMPORALITY_CUMULATIVE,
        };
    }

    private function resourceKey(Resource $resource): string
    {
        $attributes = $resource->all();
        ksort($attributes);

        return md5(json_encode($attributes, JSON_THROW_ON_ERROR));
    }

    private function setCounterData(ProtoMetric $protoMetric, Metric $metric, bool $isMonotonic): ProtoMetric
    {
        $sum = new Sum();
        $sum->setIsMonotonic($isMonotonic);
        $sum->setAggregationTemporality($this->mapTemporality($metric));

        $dataPoint = $this->createNumberDataPoint($metric);
        $sum->setDataPoints([$dataPoint]);
        $protoMetric->setSum($sum);

        return $protoMetric;
    }

    private function setGaugeData(ProtoMetric $protoMetric, Metric $metric): ProtoMetric
    {
        $gauge = new Gauge();

        $dataPoint = $this->createNumberDataPoint($metric);
        $gauge->setDataPoints([$dataPoint]);
        $protoMetric->setGauge($gauge);

        return $protoMetric;
    }

    private function setHistogramData(ProtoMetric $protoMetric, Metric $metric): ProtoMetric
    {
        $histogram = new Histogram();
        $histogram->setAggregationTemporality($this->mapTemporality($metric));

        $dataPoint = $this->createHistogramDataPoint($metric);
        $histogram->setDataPoints([$dataPoint]);
        $protoMetric->setHistogram($histogram);

        return $protoMetric;
    }

    private function toNanoseconds(DateTimeImmutable $dateTime): int
    {
        $seconds = (int) $dateTime->format('U');
        $microseconds = (int) $dateTime->format('u');

        return ($seconds * 1_000_000_000) + ($microseconds * 1_000);
    }
}
