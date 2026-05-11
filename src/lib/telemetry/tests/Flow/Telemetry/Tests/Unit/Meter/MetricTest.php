<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Meter\AggregationTemporality;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class MetricTest extends TestCase
{
    public static function attributeTypeProvider(): \Generator
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

    public static function metricTypeProvider(): \Generator
    {
        yield 'counter' => [MetricType::COUNTER];
        yield 'gauge' => [MetricType::GAUGE];
        yield 'histogram' => [MetricType::HISTOGRAM];
        yield 'up_down_counter' => [MetricType::UP_DOWN_COUNTER];
    }

    public static function valueTypeProvider(): \Generator
    {
        yield 'integer' => [42];
        yield 'zero' => [0];
        yield 'negative integer' => [-10];
        yield 'float' => [3.14159];
        yield 'negative float' => [-99.9];
    }

    #[DataProvider('valueTypeProvider')]
    public function test_accepts_int_and_float_values(int|float $value): void
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

        static::assertSame($value, $metric->value);
    }

    /**
     * @param array<string, bool|float|int|string> $attributes
     */
    #[DataProvider('attributeTypeProvider')]
    public function test_accepts_various_attribute_types(array $attributes): void
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

        static::assertSame($attributes, $metric->attributes->normalize());
    }

    #[DataProvider('metricTypeProvider')]
    public function test_creates_metric_for_each_type(MetricType $type): void
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

        static::assertSame($type, $metric->type);
    }

    public function test_creates_metric_with_all_properties(): void
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

        static::assertSame('http.requests', $metric->name);
        static::assertSame(MetricType::COUNTER, $metric->type);
        static::assertSame(42, $metric->value);
        static::assertSame(['http.method' => 'GET', 'http.status' => 200], $metric->attributes->normalize());
        static::assertSame($timestamp, $metric->timestamp);
        static::assertSame($resource, $metric->resource);
        static::assertSame($scope, $metric->scope);
        static::assertSame('requests', $metric->unit);
        static::assertSame('Total HTTP requests', $metric->description);
    }

    public function test_creates_metric_with_minimal_properties(): void
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

        static::assertSame('cpu.usage', $metric->name);
        static::assertSame(MetricType::GAUGE, $metric->type);
        static::assertSame(75.5, $metric->value);
        static::assertSame([], $metric->attributes->normalize());
        static::assertSame($timestamp, $metric->timestamp);
        static::assertSame($resource, $metric->resource);
        static::assertSame($scope, $metric->scope);
        static::assertNull($metric->unit);
        static::assertNull($metric->description);
    }

    public function test_from_array_with_minimal_data(): void
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

        static::assertSame('minimal.metric', $metric->name);
        static::assertSame(MetricType::GAUGE, $metric->type);
        static::assertSame(50, $metric->value);
        static::assertSame([], $metric->attributes->normalize());
        static::assertSame('test-scope', $metric->scope->name);
        static::assertNull($metric->unit);
        static::assertNull($metric->description);
    }

    public function test_normalize_from_array_round_trip(): void
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

        static::assertSame($original->name, $restored->name);
        static::assertSame($original->type, $restored->type);
        static::assertSame($original->value, $restored->value);
        static::assertSame($original->attributes->normalize(), $restored->attributes->normalize());
        static::assertEquals($original->timestamp, $restored->timestamp);
        static::assertSame($original->scope->name, $restored->scope->name);
        static::assertSame($original->unit, $restored->unit);
        static::assertSame($original->description, $restored->description);
    }

    public function test_normalize_returns_array_representation(): void
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

        static::assertSame('memory.usage', $normalized['name']);
        static::assertSame('gauge', $normalized['type']);
        static::assertSame(1024.5, $normalized['value']);
        static::assertSame(['process.name' => 'php'], $normalized['attributes']);
        static::assertSame('2024-01-15T10:30:00+00:00', $normalized['timestamp']);
        static::assertSame('test-scope', $normalized['scope']['name']);
        static::assertSame('2.0.0', $normalized['scope']['version']);
        static::assertSame('bytes', $normalized['unit']);
        static::assertSame('Memory usage in bytes', $normalized['description']);
        static::assertSame(AggregationTemporality::CUMULATIVE->value, $normalized['temporality']);
    }
}
