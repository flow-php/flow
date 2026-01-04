<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter;

use Flow\Telemetry\{Attributes, InstrumentationScope};
use Flow\Telemetry\Meter\{AggregationTemporality, Metric, MetricType};
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, ResourceMother};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class MetricTest extends TestCase
{
    public static function attributeTypeProvider() : \Generator
    {
        yield 'string attribute' => [['key' => 'value']];
        yield 'int attribute' => [['count' => 100]];
        yield 'float attribute' => [['ratio' => 0.5]];
        yield 'bool attribute' => [['enabled' => true]];
        yield 'mixed attributes' => [[
            'string' => 'text',
            'int' => 42,
            'float' => 3.14,
            'bool' => false,
        ]];
    }

    public static function metricTypeProvider() : \Generator
    {
        yield 'counter' => [MetricType::COUNTER];
        yield 'gauge' => [MetricType::GAUGE];
        yield 'histogram' => [MetricType::HISTOGRAM];
        yield 'up_down_counter' => [MetricType::UP_DOWN_COUNTER];
    }

    public static function valueTypeProvider() : \Generator
    {
        yield 'integer' => [42];
        yield 'zero' => [0];
        yield 'negative integer' => [-10];
        yield 'float' => [3.14159];
        yield 'negative float' => [-99.9];
    }

    #[DataProvider('valueTypeProvider')]
    public function test_accepts_int_and_float_values(int|float $value) : void
    {
        $metric = new Metric(
            name: 'test.metric',
            type: MetricType::GAUGE,
            value: $value,
            attributes: Attributes::empty(),
            timestamp: new \DateTimeImmutable(),
            resource: ResourceMother::default(),
            scope: InstrumentationScopeMother::default(),
        );

        self::assertSame($value, $metric->value);
    }

    /**
     * @param array<string, bool|float|int|string> $attributes
     */
    #[DataProvider('attributeTypeProvider')]
    public function test_accepts_various_attribute_types(array $attributes) : void
    {
        $metric = new Metric(
            name: 'test.metric',
            type: MetricType::COUNTER,
            value: 1,
            attributes: Attributes::create($attributes),
            timestamp: new \DateTimeImmutable(),
            resource: ResourceMother::default(),
            scope: InstrumentationScopeMother::default(),
        );

        self::assertSame($attributes, $metric->attributes->normalize());
    }

    #[DataProvider('metricTypeProvider')]
    public function test_creates_metric_for_each_type(MetricType $type) : void
    {
        $metric = new Metric(
            name: 'test.metric',
            type: $type,
            value: 1,
            attributes: Attributes::empty(),
            timestamp: new \DateTimeImmutable(),
            resource: ResourceMother::default(),
            scope: InstrumentationScopeMother::default(),
        );

        self::assertSame($type, $metric->type);
    }

    public function test_creates_metric_with_all_properties() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');
        $resource = ResourceMother::default();
        $scope = InstrumentationScopeMother::default();
        $metric = new Metric(
            name: 'http.requests',
            type: MetricType::COUNTER,
            value: 42,
            attributes: Attributes::create(['http.method' => 'GET', 'http.status' => 200]),
            timestamp: $timestamp,
            resource: $resource,
            scope: $scope,
            unit: 'requests',
            description: 'Total HTTP requests',
        );

        self::assertSame('http.requests', $metric->name);
        self::assertSame(MetricType::COUNTER, $metric->type);
        self::assertSame(42, $metric->value);
        self::assertSame(['http.method' => 'GET', 'http.status' => 200], $metric->attributes->normalize());
        self::assertSame($timestamp, $metric->timestamp);
        self::assertSame($resource, $metric->resource);
        self::assertSame($scope, $metric->scope);
        self::assertSame('requests', $metric->unit);
        self::assertSame('Total HTTP requests', $metric->description);
    }

    public function test_creates_metric_with_minimal_properties() : void
    {
        $timestamp = new \DateTimeImmutable();
        $resource = ResourceMother::default();
        $scope = InstrumentationScopeMother::default();
        $metric = new Metric(
            name: 'cpu.usage',
            type: MetricType::GAUGE,
            value: 75.5,
            attributes: Attributes::empty(),
            timestamp: $timestamp,
            resource: $resource,
            scope: $scope,
        );

        self::assertSame('cpu.usage', $metric->name);
        self::assertSame(MetricType::GAUGE, $metric->type);
        self::assertSame(75.5, $metric->value);
        self::assertSame([], $metric->attributes->normalize());
        self::assertSame($timestamp, $metric->timestamp);
        self::assertSame($resource, $metric->resource);
        self::assertSame($scope, $metric->scope);
        self::assertNull($metric->unit);
        self::assertNull($metric->description);
    }

    public function test_from_array_with_minimal_data() : void
    {
        $data = [
            'name' => 'minimal.metric',
            'type' => 'gauge',
            'value' => 50,
            'attributes' => [],
            'timestamp' => '2024-01-01T12:00:00+00:00',
            'resource' => [
                'attributes' => [],
            ],
            'scope' => [
                'name' => 'test-scope',
                'version' => '1.0.0',
                'schemaUrl' => null,
                'attributes' => [],
            ],
            'unit' => null,
            'description' => null,
        ];

        $metric = Metric::fromArray($data);

        self::assertSame('minimal.metric', $metric->name);
        self::assertSame(MetricType::GAUGE, $metric->type);
        self::assertSame(50, $metric->value);
        self::assertSame([], $metric->attributes->normalize());
        self::assertSame('test-scope', $metric->scope->name);
        self::assertNull($metric->unit);
        self::assertNull($metric->description);
    }

    public function test_normalize_from_array_round_trip() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');
        $resource = ResourceMother::default();
        $scope = InstrumentationScopeMother::default();
        $original = new Metric(
            name: 'http.requests',
            type: MetricType::COUNTER,
            value: 42,
            attributes: Attributes::create(['http.method' => 'GET', 'http.status' => 200]),
            timestamp: $timestamp,
            resource: $resource,
            scope: $scope,
            unit: 'requests',
            description: 'Total HTTP requests',
        );

        $normalized = $original->normalize();
        $restored = Metric::fromArray($normalized);

        self::assertSame($original->name, $restored->name);
        self::assertSame($original->type, $restored->type);
        self::assertSame($original->value, $restored->value);
        self::assertSame($original->attributes->normalize(), $restored->attributes->normalize());
        self::assertEquals($original->timestamp, $restored->timestamp);
        self::assertSame($original->scope->name, $restored->scope->name);
        self::assertSame($original->unit, $restored->unit);
        self::assertSame($original->description, $restored->description);
    }

    public function test_normalize_returns_array_representation() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');
        $scope = new InstrumentationScope('test-scope', '2.0.0');
        $metric = new Metric(
            name: 'memory.usage',
            type: MetricType::GAUGE,
            value: 1024.5,
            attributes: Attributes::create(['process.name' => 'php']),
            timestamp: $timestamp,
            resource: ResourceMother::default(),
            scope: $scope,
            unit: 'bytes',
            description: 'Memory usage in bytes',
        );

        $normalized = $metric->normalize();

        self::assertSame('memory.usage', $normalized['name']);
        self::assertSame('gauge', $normalized['type']);
        self::assertSame(1024.5, $normalized['value']);
        self::assertSame(['process.name' => 'php'], $normalized['attributes']);
        self::assertSame('2024-01-15T10:30:00+00:00', $normalized['timestamp']);
        self::assertSame('test-scope', $normalized['scope']['name']);
        self::assertSame('2.0.0', $normalized['scope']['version']);
        self::assertSame('bytes', $normalized['unit']);
        self::assertSame('Memory usage in bytes', $normalized['description']);
        self::assertSame(AggregationTemporality::CUMULATIVE->value, $normalized['temporality']);
    }
}
