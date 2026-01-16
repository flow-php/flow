<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Serializer;

use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Telemetry\{Attributes, InstrumentationScope, Resource};
use Flow\Telemetry\Context\{SpanId, TraceFlags, TraceId};
use Flow\Telemetry\Logger\{LogEntry, LogRecord, Severity};
use Flow\Telemetry\Meter\{Metric, MetricType};
use Flow\Telemetry\Tracer\{GenericEvent, Span, SpanContext, SpanKind, SpanLink, SpanStatus};
use PHPUnit\Framework\TestCase;

final class JsonSerializerTest extends TestCase
{
    private JsonSerializer $serializer;

    protected function setUp() : void
    {
        $this->serializer = new JsonSerializer();
    }

    public function test_serialize_logs_basic() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $logRecord = new LogRecord(Severity::INFO, 'Test log message', Attributes::create(['key' => 'value']));
        $entry = new LogEntry($logRecord, $resource, $scope, new \DateTimeImmutable('@1704110400.123456'));

        $json = $this->serializer->serializeLogs([$entry]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);

        self::assertArrayHasKey('resourceLogs', $data);
        /** @var array<int, array<string, mixed>> $resourceLogs */
        $resourceLogs = $data['resourceLogs'];
        self::assertCount(1, $resourceLogs);
        self::assertArrayHasKey('resource', $resourceLogs[0]);
        self::assertArrayHasKey('scopeLogs', $resourceLogs[0]);
        /** @var array<int, array<string, mixed>> $scopeLogs */
        $scopeLogs = $resourceLogs[0]['scopeLogs'];
        self::assertCount(1, $scopeLogs);
        self::assertArrayHasKey('logRecords', $scopeLogs[0]);
        /** @var array<int, array<string, mixed>> $logRecords */
        $logRecords = $scopeLogs[0]['logRecords'];
        self::assertCount(1, $logRecords);

        $record = $logRecords[0];
        self::assertSame(9, $record['severityNumber']);
        self::assertSame('INFO', $record['severityText']);
        self::assertSame(['stringValue' => 'Test log message'], $record['body']);
    }

    public function test_serialize_logs_with_span_context() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $traceId = TraceId::fromHex('0102030405060708090a0b0c0d0e0f10');
        $spanId = SpanId::fromHex('0102030405060708');
        $spanContext = SpanContext::create($traceId, $spanId, null, TraceFlags::sampled());
        $logRecord = new LogRecord(Severity::ERROR, 'Error occurred', Attributes::create([]));
        $entry = new LogEntry($logRecord, $resource, $scope, new \DateTimeImmutable('@1704110400'), $spanContext);

        $json = $this->serializer->serializeLogs([$entry]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceLogs */
        $resourceLogs = $data['resourceLogs'];
        /** @var array<int, array<string, mixed>> $scopeLogs */
        $scopeLogs = $resourceLogs[0]['scopeLogs'];
        /** @var array<int, array<string, mixed>> $logRecords */
        $logRecords = $scopeLogs[0]['logRecords'];

        $record = $logRecords[0];
        self::assertSame('0102030405060708090a0b0c0d0e0f10', $record['traceId']);
        self::assertSame('0102030405060708', $record['spanId']);
        self::assertSame(1, $record['flags']);
    }

    public function test_serialize_metrics_counter() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $metric = new Metric(
            name: 'requests.total',
            type: MetricType::COUNTER,
            value: 42,
            attributes: Attributes::create(['http.method' => 'GET']),
            timestamp: new \DateTimeImmutable(),
            resource: $resource,
            scope: $scope,
            unit: 'requests',
            description: 'Total request count',
        );

        $json = $this->serializer->serializeMetrics([$metric]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);

        self::assertArrayHasKey('resourceMetrics', $data);
        /** @var array<int, array<string, mixed>> $resourceMetrics */
        $resourceMetrics = $data['resourceMetrics'];
        self::assertCount(1, $resourceMetrics);
        self::assertArrayHasKey('scopeMetrics', $resourceMetrics[0]);
        /** @var array<int, array<string, mixed>> $scopeMetrics */
        $scopeMetrics = $resourceMetrics[0]['scopeMetrics'];
        self::assertCount(1, $scopeMetrics);
        self::assertArrayHasKey('metrics', $scopeMetrics[0]);
        /** @var array<int, array<string, mixed>> $metrics */
        $metrics = $scopeMetrics[0]['metrics'];
        self::assertCount(1, $metrics);

        $serializedMetric = $metrics[0];
        self::assertSame('requests.total', $serializedMetric['name']);
        self::assertSame('Total request count', $serializedMetric['description']);
        self::assertSame('requests', $serializedMetric['unit']);
        self::assertArrayHasKey('sum', $serializedMetric);
        /** @var array<string, mixed> $sum */
        $sum = $serializedMetric['sum'];
        self::assertTrue($sum['isMonotonic']);
    }

    public function test_serialize_metrics_gauge() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $metric = new Metric(
            name: 'memory.usage',
            type: MetricType::GAUGE,
            value: 1024.5,
            attributes: Attributes::create([]),
            timestamp: new \DateTimeImmutable(),
            resource: $resource,
            scope: $scope,
            unit: 'bytes',
        );

        $json = $this->serializer->serializeMetrics([$metric]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceMetrics */
        $resourceMetrics = $data['resourceMetrics'];
        /** @var array<int, array<string, mixed>> $scopeMetrics */
        $scopeMetrics = $resourceMetrics[0]['scopeMetrics'];
        /** @var array<int, array<string, mixed>> $metricsArray */
        $metricsArray = $scopeMetrics[0]['metrics'];

        $serializedMetric = $metricsArray[0];
        self::assertSame('memory.usage', $serializedMetric['name']);
        self::assertArrayHasKey('gauge', $serializedMetric);
        /** @var array<string, mixed> $gauge */
        $gauge = $serializedMetric['gauge'];
        /** @var array<int, array<string, mixed>> $dataPoints */
        $dataPoints = $gauge['dataPoints'];
        self::assertCount(1, $dataPoints);
        self::assertSame(1024.5, $dataPoints[0]['asDouble']);
    }

    public function test_serialize_metrics_histogram() : void
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
            timestamp: new \DateTimeImmutable(),
            resource: $resource,
            scope: $scope,
            unit: 'ms',
        );

        $json = $this->serializer->serializeMetrics([$metric]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceMetrics */
        $resourceMetrics = $data['resourceMetrics'];
        /** @var array<int, array<string, mixed>> $scopeMetrics */
        $scopeMetrics = $resourceMetrics[0]['scopeMetrics'];
        /** @var array<int, array<string, mixed>> $metricsArray */
        $metricsArray = $scopeMetrics[0]['metrics'];

        $serializedMetric = $metricsArray[0];
        self::assertSame('request.duration', $serializedMetric['name']);
        self::assertArrayHasKey('histogram', $serializedMetric);
        /** @var array<string, mixed> $histogram */
        $histogram = $serializedMetric['histogram'];
        /** @var array<int, array<string, mixed>> $dataPoints */
        $dataPoints = $histogram['dataPoints'];
        $dataPoint = $dataPoints[0];
        self::assertSame(['1', '1', '1', '1'], $dataPoint['bucketCounts']);
        self::assertEquals([10.0, 50.0, 100.0], $dataPoint['explicitBounds']);
    }

    public function test_serialize_metrics_up_down_counter() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $metric = new Metric(
            name: 'queue.size',
            type: MetricType::UP_DOWN_COUNTER,
            value: 10,
            attributes: Attributes::create([]),
            timestamp: new \DateTimeImmutable(),
            resource: $resource,
            scope: $scope,
        );

        $json = $this->serializer->serializeMetrics([$metric]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceMetrics */
        $resourceMetrics = $data['resourceMetrics'];
        /** @var array<int, array<string, mixed>> $scopeMetrics */
        $scopeMetrics = $resourceMetrics[0]['scopeMetrics'];
        /** @var array<int, array<string, mixed>> $metricsArray */
        $metricsArray = $scopeMetrics[0]['metrics'];

        $serializedMetric = $metricsArray[0];
        self::assertSame('queue.size', $serializedMetric['name']);
        self::assertArrayHasKey('sum', $serializedMetric);
        /** @var array<string, mixed> $sum */
        $sum = $serializedMetric['sum'];
        self::assertFalse($sum['isMonotonic']);
    }

    public function test_serialize_produces_valid_json() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new \DateTimeImmutable(), $resource, $scope);

        $json = $this->serializer->serializeSpans([$span]);

        self::assertJson($json);
    }

    public function test_serialize_spans_basic() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $traceId = TraceId::fromHex('0102030405060708090a0b0c0d0e0f10');
        $spanId = SpanId::fromHex('0102030405060708');
        $context = SpanContext::create($traceId, $spanId);
        $startTime = new \DateTimeImmutable('2024-01-01 12:00:00.000000');

        $span = new Span('test-span', $context, SpanKind::INTERNAL, $startTime, $resource, $scope);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);

        self::assertArrayHasKey('resourceSpans', $data);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        self::assertCount(1, $resourceSpans);
        self::assertArrayHasKey('resource', $resourceSpans[0]);
        self::assertArrayHasKey('scopeSpans', $resourceSpans[0]);
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        self::assertCount(1, $scopeSpans);
        self::assertArrayHasKey('spans', $scopeSpans[0]);
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];
        self::assertCount(1, $spansArray);

        $serializedSpan = $spansArray[0];
        self::assertSame('0102030405060708090a0b0c0d0e0f10', $serializedSpan['traceId']);
        self::assertSame('0102030405060708', $serializedSpan['spanId']);
        self::assertSame('test-span', $serializedSpan['name']);
        self::assertSame(1, $serializedSpan['kind']);
    }

    public function test_serialize_spans_resource_and_scope() : void
    {
        $resource = Resource::create([
            'service.name' => 'test-service',
            'service.version' => '1.0.0',
        ]);
        $scope = new InstrumentationScope('flow-php', '2.0.0', null, Attributes::create(['scope.key' => 'scope.value']));
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());
        $span = new Span('test-span', $context, SpanKind::INTERNAL, new \DateTimeImmutable(), $resource, $scope);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<string, mixed> $resourceData */
        $resourceData = $resourceSpans[0]['resource'];
        /** @var array<int, mixed> $resourceAttributes */
        $resourceAttributes = $resourceData['attributes'];
        self::assertCount(2, $resourceAttributes);

        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<string, mixed> $scopeData */
        $scopeData = $scopeSpans[0]['scope'];
        self::assertSame('flow-php', $scopeData['name']);
        self::assertSame('2.0.0', $scopeData['version']);
        /** @var array<int, mixed> $scopeAttributes */
        $scopeAttributes = $scopeData['attributes'];
        self::assertCount(1, $scopeAttributes);
    }

    public function test_serialize_spans_span_kinds() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');

        $kindTests = [
            [SpanKind::INTERNAL, 1],
            [SpanKind::SERVER, 2],
            [SpanKind::CLIENT, 3],
            [SpanKind::PRODUCER, 4],
            [SpanKind::CONSUMER, 5],
        ];

        foreach ($kindTests as [$kind, $expectedValue]) {
            $context = SpanContext::create(TraceId::generate(), SpanId::generate());
            $span = new Span('test-span', $context, $kind, new \DateTimeImmutable(), $resource, $scope);

            $json = $this->serializer->serializeSpans([$span]);
            /** @var array<string, mixed> $data */
            $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
            /** @var array<int, array<string, mixed>> $resourceSpans */
            $resourceSpans = $data['resourceSpans'];
            /** @var array<int, array<string, mixed>> $scopeSpans */
            $scopeSpans = $resourceSpans[0]['scopeSpans'];
            /** @var array<int, array<string, mixed>> $spansArray */
            $spansArray = $scopeSpans[0]['spans'];

            $serializedSpan = $spansArray[0];
            self::assertSame($expectedValue, $serializedSpan['kind'], "SpanKind {$kind->name} should be serialized as {$expectedValue}");
        }
    }

    public function test_serialize_spans_with_array_attributes() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new \DateTimeImmutable(), $resource, $scope);
        $span->setAttribute('tags', ['foo', 'bar', 'baz']);
        $span->setAttribute('ports', [80, 443, 8080]);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
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
        self::assertArrayHasKey('arrayValue', $tagsValue);
        /** @var array<string, mixed> $tagsArrayValue */
        $tagsArrayValue = $tagsValue['arrayValue'];
        /** @var array<int, mixed> $tagsValues */
        $tagsValues = $tagsArrayValue['values'];
        self::assertCount(3, $tagsValues);
        self::assertSame(['stringValue' => 'foo'], $tagsValues[0]);

        /** @var array<string, mixed> $portsValue */
        $portsValue = $attributeMap['ports'];
        self::assertArrayHasKey('arrayValue', $portsValue);
        /** @var array<string, mixed> $portsArrayValue */
        $portsArrayValue = $portsValue['arrayValue'];
        /** @var array<int, mixed> $portsValues */
        $portsValues = $portsArrayValue['values'];
        self::assertCount(3, $portsValues);
        self::assertSame(['intValue' => '80'], $portsValues[0]);
    }

    public function test_serialize_spans_with_attributes() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new \DateTimeImmutable(), $resource, $scope);
        $span->setAttribute('http.method', 'GET');
        $span->setAttribute('http.status_code', 200);
        $span->setAttribute('duration', 42.5);
        $span->setAttribute('enabled', true);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        /** @var array<int, array{key: string, value: mixed}> $attributes */
        $attributes = $serializedSpan['attributes'];
        self::assertCount(4, $attributes);

        /** @var array<string, mixed> $attributeMap */
        $attributeMap = [];

        foreach ($attributes as $attr) {
            $attributeMap[$attr['key']] = $attr['value'];
        }

        self::assertSame(['stringValue' => 'GET'], $attributeMap['http.method']);
        self::assertSame(['intValue' => '200'], $attributeMap['http.status_code']);
        self::assertSame(['doubleValue' => 42.5], $attributeMap['duration']);
        self::assertSame(['boolValue' => true], $attributeMap['enabled']);
    }

    public function test_serialize_spans_with_events() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new \DateTimeImmutable(), $resource, $scope);
        $span->recordEvent(GenericEvent::create('cache.hit', new \DateTimeImmutable(), ['key' => 'user:123']));

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        /** @var array<int, array<string, mixed>> $events */
        $events = $serializedSpan['events'];
        self::assertCount(1, $events);
        self::assertSame('cache.hit', $events[0]['name']);
        self::assertArrayHasKey('timeUnixNano', $events[0]);
        /** @var array<int, mixed> $eventAttributes */
        $eventAttributes = $events[0]['attributes'];
        self::assertCount(1, $eventAttributes);
    }

    public function test_serialize_spans_with_links() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $linkedTraceId = TraceId::generate();
        $linkedSpanId = SpanId::generate();
        $linkedContext = SpanContext::create($linkedTraceId, $linkedSpanId);

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new \DateTimeImmutable(), $resource, $scope);
        $span->addLink(SpanLink::create($linkedContext, ['reason' => 'batch']));

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        /** @var array<int, array<string, mixed>> $links */
        $links = $serializedSpan['links'];
        self::assertCount(1, $links);
        self::assertSame($linkedTraceId->toHex(), $links[0]['traceId']);
        self::assertSame($linkedSpanId->toHex(), $links[0]['spanId']);
        /** @var array<int, mixed> $linkAttributes */
        $linkAttributes = $links[0]['attributes'];
        self::assertCount(1, $linkAttributes);
    }

    public function test_serialize_spans_with_parent() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $parentSpanId = SpanId::generate();
        $context = SpanContext::create(TraceId::generate(), SpanId::generate(), $parentSpanId);

        $span = new Span('child-span', $context, SpanKind::INTERNAL, new \DateTimeImmutable(), $resource, $scope);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        self::assertSame($parentSpanId->toHex(), $serializedSpan['parentSpanId']);
    }

    public function test_serialize_spans_with_sampled_flag() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(
            TraceId::generate(),
            SpanId::generate(),
            null,
            TraceFlags::sampled()
        );

        $span = new Span('sampled-span', $context, SpanKind::INTERNAL, new \DateTimeImmutable(), $resource, $scope);

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        self::assertSame(1, $serializedSpan['flags']);
    }

    public function test_serialize_spans_with_status() : void
    {
        $resource = Resource::create(['service.name' => 'test-service']);
        $scope = new InstrumentationScope('flow-php', '1.0.0');
        $context = SpanContext::create(TraceId::generate(), SpanId::generate());

        $span = new Span('test-span', $context, SpanKind::INTERNAL, new \DateTimeImmutable(), $resource, $scope);
        $span->setStatus(SpanStatus::error('Something went wrong'));

        $json = $this->serializer->serializeSpans([$span]);
        /** @var array<string, mixed> $data */
        $data = \json_decode($json, true, 512, \JSON_THROW_ON_ERROR);
        /** @var array<int, array<string, mixed>> $resourceSpans */
        $resourceSpans = $data['resourceSpans'];
        /** @var array<int, array<string, mixed>> $scopeSpans */
        $scopeSpans = $resourceSpans[0]['scopeSpans'];
        /** @var array<int, array<string, mixed>> $spansArray */
        $spansArray = $scopeSpans[0]['spans'];

        $serializedSpan = $spansArray[0];
        /** @var array<string, mixed> $status */
        $status = $serializedSpan['status'];
        self::assertSame(2, $status['code']);
        self::assertSame('Something went wrong', $status['message']);
    }
}
