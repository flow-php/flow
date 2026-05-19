<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Serializer;

use DateTimeImmutable;
use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceFlags;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogRecord;
use Flow\Telemetry\Logger\Severity;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tracer\GenericEvent;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\SpanKind;
use Flow\Telemetry\Tracer\SpanLink;
use Flow\Telemetry\Tracer\SpanStatus;
use PHPUnit\Framework\TestCase;

use function json_decode;

use const JSON_THROW_ON_ERROR;

final class JsonSerializerTest extends TestCase
{
    private JsonSerializer $serializer;

    protected function setUp(): void
    {
        $this->serializer = new JsonSerializer();
    }

    public function test_serialize_logs_basic(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $logRecord = new LogRecord(Severity::INFO, 'Test log message', Attributes::create(['key' => 'value']));
        $entry = new LogEntry($logRecord, $resource, $scope, new DateTimeImmutable('@1704110400.123456'));

        $json = $this->serializer->serializeLogs([$entry]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        static::assertArrayHasKey('resourceLogs', $data);
        /** @var array<int, array<string, mixed>> $resourceLogs */
        $resourceLogs = $data['resourceLogs'];
        static::assertCount(1, $resourceLogs);
        static::assertArrayHasKey('resource', $resourceLogs[0]);
        static::assertArrayHasKey('scopeLogs', $resourceLogs[0]);
        /** @var array<int, array<string, mixed>> $scopeLogs */
        $scopeLogs = $resourceLogs[0]['scopeLogs'];
        static::assertCount(1, $scopeLogs);
        static::assertArrayHasKey('logRecords', $scopeLogs[0]);
        /** @var array<int, array<string, mixed>> $logRecords */
        $logRecords = $scopeLogs[0]['logRecords'];
        static::assertCount(1, $logRecords);

        $record = $logRecords[0];
        static::assertSame(9, $record['severityNumber']);
        static::assertSame('INFO', $record['severityText']);
        static::assertSame(['stringValue' => 'Test log message'], $record['body']);
    }

    public function test_serialize_logs_with_span_context(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $traceId = TraceId::fromHex('0102030405060708090a0b0c0d0e0f10');
        $spanId = SpanId::fromHex('0102030405060708');
        $spanContext = SpanContext::create($traceId, $spanId, null, TraceFlags::sampled());
        $logRecord = new LogRecord(Severity::ERROR, 'Error occurred', Attributes::create([]));
        $entry = new LogEntry($logRecord, $resource, $scope, new DateTimeImmutable('@1704110400'), $spanContext);

        $json = $this->serializer->serializeLogs([$entry]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceLogs */
        $resourceLogs = $data['resourceLogs'];
        /** @var array<int, array<string, mixed>> $scopeLogs */
        $scopeLogs = $resourceLogs[0]['scopeLogs'];
        /** @var array<int, array<string, mixed>> $logRecords */
        $logRecords = $scopeLogs[0]['logRecords'];

        $record = $logRecords[0];
        static::assertSame('0102030405060708090a0b0c0d0e0f10', $record['traceId']);
        static::assertSame('0102030405060708', $record['spanId']);
        static::assertSame(1, $record['flags']);
    }

    public function test_serialize_metrics_counter(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $metric = new Metric(
            name: 'requests.total',
            type: MetricType::COUNTER,
            value: 42,
            attributes: Attributes::create(['http.method' => 'GET']),
            timestamp: new DateTimeImmutable(),
            resource: $resource,
            scope: $scope,
            unit: 'requests',
            description: 'Total request count',
        );

        $json = $this->serializer->serializeMetrics([$metric]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        static::assertArrayHasKey('resourceMetrics', $data);
        /** @var array<int, array<string, mixed>> $resourceMetrics */
        $resourceMetrics = $data['resourceMetrics'];
        static::assertCount(1, $resourceMetrics);
        static::assertArrayHasKey('scopeMetrics', $resourceMetrics[0]);
        /** @var array<int, array<string, mixed>> $scopeMetrics */
        $scopeMetrics = $resourceMetrics[0]['scopeMetrics'];
        static::assertCount(1, $scopeMetrics);
        static::assertArrayHasKey('metrics', $scopeMetrics[0]);
        /** @var array<int, array<string, mixed>> $metrics */
        $metrics = $scopeMetrics[0]['metrics'];
        static::assertCount(1, $metrics);

        $serializedMetric = $metrics[0];
        static::assertSame('requests.total', $serializedMetric['name']);
        static::assertSame('Total request count', $serializedMetric['description']);
        static::assertSame('requests', $serializedMetric['unit']);
        static::assertArrayHasKey('sum', $serializedMetric);
        /** @var array<string, mixed> $sum */
        $sum = $serializedMetric['sum'];
        static::assertTrue($sum['isMonotonic']);
    }

    public function test_serialize_metrics_gauge(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $metric = new Metric(
            name: 'memory.usage',
            type: MetricType::GAUGE,
            value: 1024.5,
            attributes: Attributes::create([]),
            timestamp: new DateTimeImmutable(),
            resource: $resource,
            scope: $scope,
            unit: 'bytes',
        );

        $json = $this->serializer->serializeMetrics([$metric]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceMetrics */
        $resourceMetrics = $data['resourceMetrics'];
        /** @var array<int, array<string, mixed>> $scopeMetrics */
        $scopeMetrics = $resourceMetrics[0]['scopeMetrics'];
        /** @var array<int, array<string, mixed>> $metricsArray */
        $metricsArray = $scopeMetrics[0]['metrics'];

        $serializedMetric = $metricsArray[0];
        static::assertSame('memory.usage', $serializedMetric['name']);
        static::assertArrayHasKey('gauge', $serializedMetric);
        /** @var array<string, mixed> $gauge */
        $gauge = $serializedMetric['gauge'];
        /** @var array<int, array<string, mixed>> $dataPoints */
        $dataPoints = $gauge['dataPoints'];
        static::assertCount(1, $dataPoints);
        static::assertSame(1024.5, $dataPoints[0]['asDouble']);
    }

    public function test_serialize_metrics_histogram(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $metric = new Metric(
            name: 'request.duration',
            type: MetricType::HISTOGRAM,
            value: 150.5,
            attributes: Attributes::create([
                'histogram.count' => 4,
                'histogram.sum' => 255.0,
                'histogram.min' => 5.0,
                'histogram.max' => 150.0,
                'histogram.bucketCounts' => [1, 1, 1, 1],
                'histogram.explicitBounds' => [10.0, 50.0, 100.0],
            ]),
            timestamp: new DateTimeImmutable(),
            resource: $resource,
            scope: $scope,
            unit: 'ms',
        );

        $json = $this->serializer->serializeMetrics([$metric]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceMetrics */
        $resourceMetrics = $data['resourceMetrics'];
        /** @var array<int, array<string, mixed>> $scopeMetrics */
        $scopeMetrics = $resourceMetrics[0]['scopeMetrics'];
        /** @var array<int, array<string, mixed>> $metricsArray */
        $metricsArray = $scopeMetrics[0]['metrics'];

        $serializedMetric = $metricsArray[0];
        static::assertSame('request.duration', $serializedMetric['name']);
        static::assertArrayHasKey('histogram', $serializedMetric);
        /** @var array<string, mixed> $histogram */
        $histogram = $serializedMetric['histogram'];
        /** @var array<int, array<string, mixed>> $dataPoints */
        $dataPoints = $histogram['dataPoints'];
        $dataPoint = $dataPoints[0];
        static::assertSame(['1', '1', '1', '1'], $dataPoint['bucketCounts']);
        static::assertEquals([10.0, 50.0, 100.0], $dataPoint['explicitBounds']);
    }

    public function test_serialize_metrics_up_down_counter(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $metric = new Metric(
            name: 'queue.size',
            type: MetricType::UP_DOWN_COUNTER,
            value: 10,
            attributes: Attributes::create([]),
            timestamp: new DateTimeImmutable(),
            resource: $resource,
            scope: $scope,
        );

        $json = $this->serializer->serializeMetrics([$metric]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceMetrics */
        $resourceMetrics = $data['resourceMetrics'];
        /** @var array<int, array<string, mixed>> $scopeMetrics */
        $scopeMetrics = $resourceMetrics[0]['scopeMetrics'];
        /** @var array<int, array<string, mixed>> $metricsArray */
        $metricsArray = $scopeMetrics[0]['metrics'];

        $serializedMetric = $metricsArray[0];
        static::assertSame('queue.size', $serializedMetric['name']);
        static::assertArrayHasKey('sum', $serializedMetric);
        /** @var array<string, mixed> $sum */
        $sum = $serializedMetric['sum'];
        static::assertFalse($sum['isMonotonic']);
    }

    public function test_serialize_produces_valid_json(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new DateTimeImmutable(), $resource, $scope);

        $json = $this->serializer->serializeSpans([$span]);

        static::assertJson($json);
    }

    public function test_serialize_spans_basic(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $traceId = TraceId::fromHex('0102030405060708090a0b0c0d0e0f10');
        $spanId = SpanId::fromHex('0102030405060708');
        $context = SpanContext::create($traceId, $spanId);
        $startTime = new DateTimeImmutable('2024-01-01 12:00:00.000000');

        $span = new Span('test-span', $context, SpanKind::INTERNAL, $startTime, $resource, $scope);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        static::assertArrayHasKey('resourceSpans', $data);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        static::assertCount(1, $resourceSpans);
        static::assertArrayHasKey('resource', $resourceSpans[0]);
        static::assertArrayHasKey('scopeSpans', $resourceSpans[0]);
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        static::assertCount(1, $scopeSpans);
        static::assertArrayHasKey('spans', $scopeSpans[0]);
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];
        static::assertCount(1, $spansArray);

        $serializedSpan = $spansArray[0];
        static::assertSame('0102030405060708090a0b0c0d0e0f10', $serializedSpan['traceId']);
        static::assertSame('0102030405060708', $serializedSpan['spanId']);
        static::assertSame('test-span', $serializedSpan['name']);
        static::assertSame(1, $serializedSpan['kind']);
    }

    public function test_serialize_spans_resource_and_scope(): void
    {
        $resource = Resource::create([
            'service.name' => 'test-service',
            'service.version' => '1.0.0',
        ]);
        $scope = new InstrumentationScope('flow-php', '2.0.0', null, Attributes::create([
            'scope.key' => 'scope.value',
        ]));
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $span = new Span('test-span', $context, SpanKind::INTERNAL, new DateTimeImmutable(), $resource, $scope);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<string, mixed> $resourceData */
        $resourceData = $resourceSpans[0]['resource'];
        /** @var array<int, mixed> $resourceAttributes */
        $resourceAttributes = $resourceData['attributes'];
        static::assertCount(2, $resourceAttributes);

        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<string, mixed> $scopeData */
        $scopeData = $scopeSpans[0]['scope'];
        static::assertSame('flow-php', $scopeData['name']);
        static::assertSame('2.0.0', $scopeData['version']);
        /** @var array<int, mixed> $scopeAttributes */
        $scopeAttributes = $scopeData['attributes'];
        static::assertCount(1, $scopeAttributes);
    }

    public function test_serialize_spans_span_kinds(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');

        $kindTests = [
            [SpanKind::INTERNAL, 1],
            [SpanKind::SERVER,   2],
            [SpanKind::CLIENT,   3],
            [SpanKind::PRODUCER, 4],
            [SpanKind::CONSUMER, 5],
        ];

        foreach ($kindTests as [$kind, $expectedValue]) {
            $context = SpanContext::create(TraceId::generate(), SpanId::generate());
            $span = new Span('test-span', $context, $kind, new DateTimeImmutable(), $resource, $scope);

            $json = $this->serializer->serializeSpans([$span]);
            /** @var array<string, mixed> $data */
            $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
            /** @var array<int, array<string, mixed>> $resourceSpans */
            $resourceSpans = $data['resourceSpans'];
            /** @var array<int, array<string, mixed>> $scopeSpans */
            $scopeSpans = $resourceSpans[0]['scopeSpans'];
            /** @var array<int, array<string, mixed>> $spansArray */
            $spansArray = $scopeSpans[0]['spans'];

            $serializedSpan = $spansArray[0];
            static::assertSame(
                $expectedValue,
                $serializedSpan['kind'],
                "SpanKind {$kind->name} should be serialized as {$expectedValue}",
            );
        }
    }

    public function test_serialize_spans_with_array_attributes(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new DateTimeImmutable(), $resource, $scope);
        $span->setAttribute('tags', ['foo', 'bar', 'baz']);
        $span->setAttribute('ports', [80, 443, 8080]);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        /** @var array<string, mixed> $attributeMap */
        $attributeMap = [];

        /** @var array<int, array{key: string, value: mixed}> $attributes */
        $attributes = $serializedSpan['attributes'];

        foreach ($attributes as $attr) {
            $attributeMap[$attr['key']] = $attr['value'];
        }

        /** @var array<string, mixed> $tagsValue */
        $tagsValue = $attributeMap['tags'];
        static::assertArrayHasKey('arrayValue', $tagsValue);
        /** @var array<string, mixed> $tagsArrayValue */
        $tagsArrayValue = $tagsValue['arrayValue'];
        /** @var array<int, mixed> $tagsValues */
        $tagsValues = $tagsArrayValue['values'];
        static::assertCount(3, $tagsValues);
        static::assertSame(['stringValue' => 'foo'], $tagsValues[0]);

        /** @var array<string, mixed> $portsValue */
        $portsValue = $attributeMap['ports'];
        static::assertArrayHasKey('arrayValue', $portsValue);
        /** @var array<string, mixed> $portsArrayValue */
        $portsArrayValue = $portsValue['arrayValue'];
        /** @var array<int, mixed> $portsValues */
        $portsValues = $portsArrayValue['values'];
        static::assertCount(3, $portsValues);
        static::assertSame(['intValue' => '80'], $portsValues[0]);
    }

    public function test_serialize_spans_with_attributes(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new DateTimeImmutable(), $resource, $scope);
        $span->setAttribute('http.method', 'GET');
        $span->setAttribute('http.status_code', 200);
        $span->setAttribute('duration', 42.5);
        $span->setAttribute('enabled', true);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        /** @var array<int, array{key: string, value: mixed}> $attributes */
        $attributes = $serializedSpan['attributes'];
        static::assertCount(4, $attributes);

        /** @var array<string, mixed> $attributeMap */
        $attributeMap = [];

        foreach ($attributes as $attr) {
            $attributeMap[$attr['key']] = $attr['value'];
        }

        static::assertSame(['stringValue' => 'GET'], $attributeMap['http.method']);
        static::assertSame(['intValue' => '200'], $attributeMap['http.status_code']);
        static::assertSame(['doubleValue' => 42.5], $attributeMap['duration']);
        static::assertSame(['boolValue' => true], $attributeMap['enabled']);
    }

    public function test_serialize_spans_with_events(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new DateTimeImmutable(), $resource, $scope);
        $span->recordEvent(GenericEvent::create('cache.hit', new DateTimeImmutable(), ['key' => 'user:123']));

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        /** @var array<int, array<string, mixed>> $events */
        $events = $serializedSpan['events'];
        static::assertCount(1, $events);
        static::assertSame('cache.hit', $events[0]['name']);
        static::assertArrayHasKey('timeUnixNano', $events[0]);
        /** @var array<int, mixed> $eventAttributes */
        $eventAttributes = $events[0]['attributes'];
        static::assertCount(1, $eventAttributes);
    }

    public function test_serialize_spans_with_links(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $linkedTraceId = TraceId::generate();
        $linkedSpanId = SpanId::generate();
        $linkedContext = SpanContext::create($linkedTraceId, $linkedSpanId);

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new DateTimeImmutable(), $resource, $scope);
        $span->addLink(SpanLink::create($linkedContext, ['reason' => 'batch']));

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        /** @var array<int, array<string, mixed>> $links */
        $links = $serializedSpan['links'];
        static::assertCount(1, $links);
        static::assertSame($linkedTraceId->toHex(), $links[0]['traceId']);
        static::assertSame($linkedSpanId->toHex(), $links[0]['spanId']);
        /** @var array<int, mixed> $linkAttributes */
        $linkAttributes = $links[0]['attributes'];
        static::assertCount(1, $linkAttributes);
    }

    public function test_serialize_spans_with_parent(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $parentSpanId = SpanId::generate();
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), $parentSpanId);

        $span = new Span('child-span', $context, SpanKind::INTERNAL, new DateTimeImmutable(), $resource, $scope);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        static::assertSame($parentSpanId->toHex(), $serializedSpan['parentSpanId']);
    }

    public function test_serialize_spans_with_sampled_flag(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), null, TraceFlags::sampled());

        $span = new Span('sampled-span', $context, SpanKind::INTERNAL, new DateTimeImmutable(), $resource, $scope);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        static::assertSame(1, $serializedSpan['flags']);
    }

    public function test_serialize_spans_with_status(): void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new DateTimeImmutable(), $resource, $scope);
        $span->setStatus(SpanStatus::error('Something went wrong'));

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        /** @var array<string, mixed> $status */
        $status = $serializedSpan['status'];
        static::assertSame(2, $status['code']);
        static::assertSame('Something went wrong', $status['message']);
    }
}
