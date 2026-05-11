<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Unit\Serializer;

use Flow\Bridge\Telemetry\OTLP\Serializer\MetricSerializer;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class MetricSerializerTest extends TestCase
{
    private Resource $resource;

    private InstrumentationScope $scope;

    private MetricSerializer $serializer;

    protected function setUp(): void
    {
        $this->serializer = new MetricSerializer();
        $this->resource = ResourceMother::default();
        $this->scope = new InstrumentationScope('flow-php', '1.0.0');
    }

    public function test_serialize_counter(): void
    {
        $metric = new Metric(
            name: 'requests.total',
            type: MetricType::COUNTER,
            value: 42,
            attributes: Attributes::create(['http.method' => 'GET']),
            timestamp: new \DateTimeImmutable(),
            resource: $this->resource,
            scope: $this->scope,
            unit: 'requests',
            description: 'Total request count',
        );

        $result = $this->serializer->serialize($metric);

        static::assertSame('requests.total', $result['name']);
        static::assertSame('Total request count', $result['description']);
        static::assertSame('requests', $result['unit']);
        static::assertArrayHasKey('sum', $result);
        /** @var array<string, mixed> $sum */
        $sum = $result['sum'];
        static::assertTrue($sum['isMonotonic']);
        static::assertSame(2, $sum['aggregationTemporality']);
    }

    public function test_serialize_gauge(): void
    {
        $metric = new Metric(
            name: 'memory.usage',
            type: MetricType::GAUGE,
            value: 1024.5,
            attributes: Attributes::create([]),
            timestamp: new \DateTimeImmutable(),
            resource: $this->resource,
            scope: $this->scope,
            unit: 'bytes',
        );

        $result = $this->serializer->serialize($metric);

        static::assertSame('memory.usage', $result['name']);
        static::assertArrayHasKey('gauge', $result);
        /** @var array<string, mixed> $gauge */
        $gauge = $result['gauge'];
        /** @var array<int, mixed> $dataPoints */
        $dataPoints = $gauge['dataPoints'];
        static::assertCount(1, $dataPoints);
    }

    public function test_serialize_histogram(): void
    {
        $metric = new Metric(
            name: 'request.duration',
            type: MetricType::HISTOGRAM,
            value: 150.5,
            attributes: Attributes::create([]),
            timestamp: new \DateTimeImmutable(),
            resource: $this->resource,
            scope: $this->scope,
            unit: 'ms',
        );

        $result = $this->serializer->serialize($metric);

        static::assertSame('request.duration', $result['name']);
        static::assertArrayHasKey('histogram', $result);
        /** @var array<string, mixed> $histogram */
        $histogram = $result['histogram'];
        /** @var array<int, array<string, mixed>> $dataPoints */
        $dataPoints = $histogram['dataPoints'];
        static::assertSame('1', $dataPoints[0]['count']);
    }

    public function test_serialize_up_down_counter(): void
    {
        $metric = new Metric(
            name: 'queue.size',
            type: MetricType::UP_DOWN_COUNTER,
            value: 10,
            attributes: Attributes::create([]),
            timestamp: new \DateTimeImmutable(),
            resource: $this->resource,
            scope: $this->scope,
        );

        $result = $this->serializer->serialize($metric);

        static::assertSame('queue.size', $result['name']);
        static::assertArrayHasKey('sum', $result);
        /** @var array<string, mixed> $sum */
        $sum = $result['sum'];
        static::assertFalse($sum['isMonotonic']);
    }

    public function test_serialize_with_float_value(): void
    {
        $metric = new Metric(
            name: 'cpu.usage',
            type: MetricType::GAUGE,
            value: 75.5,
            attributes: Attributes::create([]),
            timestamp: new \DateTimeImmutable(),
            resource: $this->resource,
            scope: $this->scope,
        );

        $result = $this->serializer->serialize($metric);

        /** @var array<string, mixed> $gauge */
        $gauge = $result['gauge'];
        /** @var array<int, array<string, mixed>> $dataPoints */
        $dataPoints = $gauge['dataPoints'];
        static::assertSame(75.5, $dataPoints[0]['asDouble']);
    }

    public function test_serialize_with_integer_value(): void
    {
        $metric = new Metric(
            name: 'items.count',
            type: MetricType::COUNTER,
            value: 100,
            attributes: Attributes::create([]),
            timestamp: new \DateTimeImmutable(),
            resource: $this->resource,
            scope: $this->scope,
        );

        $result = $this->serializer->serialize($metric);

        /** @var array<string, mixed> $sum */
        $sum = $result['sum'];
        /** @var array<int, array<string, mixed>> $dataPoints */
        $dataPoints = $sum['dataPoints'];
        static::assertSame('100', $dataPoints[0]['asInt']);
    }

    public function test_serialize_without_optional_fields(): void
    {
        $metric = new Metric(
            name: 'simple.metric',
            type: MetricType::GAUGE,
            value: 1,
            attributes: Attributes::create([]),
            timestamp: new \DateTimeImmutable(),
            resource: $this->resource,
            scope: $this->scope,
        );

        $result = $this->serializer->serialize($metric);

        static::assertArrayNotHasKey('description', $result);
        static::assertArrayNotHasKey('unit', $result);
    }
}
