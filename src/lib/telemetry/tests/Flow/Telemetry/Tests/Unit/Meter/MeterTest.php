<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter;

use DateTimeImmutable;
use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Meter\Instrument\Counter;
use Flow\Telemetry\Meter\Instrument\Gauge;
use Flow\Telemetry\Meter\Instrument\Histogram;
use Flow\Telemetry\Meter\Instrument\UpDownCounter;
use Flow\Telemetry\Meter\Meter;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Generator;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_filter;
use function array_values;

final class MeterTest extends TestCase
{
    public static function gaugeValueProvider(): Generator
    {
        yield 'zero' => [0];
        yield 'positive' => [100];
        yield 'negative' => [-50];
        yield 'float' => [3.14159];
    }

    public static function histogramValueProvider(): Generator
    {
        yield 'zero' => [0];
        yield 'small' => [0.001];
        yield 'medium' => [500];
        yield 'large' => [10000.5];
    }

    public static function negativeAmountProvider(): Generator
    {
        yield 'negative integer' => [-1];
        yield 'negative float' => [-0.5];
        yield 'large negative' => [-1000];
    }

    public static function upDownCounterValueProvider(): Generator
    {
        yield 'positive' => [10];
        yield 'negative' => [-10];
        yield 'zero' => [0];
        yield 'positive float' => [5.5];
        yield 'negative float' => [-5.5];
    }

    public static function validCounterAmountProvider(): Generator
    {
        yield 'zero' => [0];
        yield 'positive integer' => [42];
        yield 'positive float' => [3.14];
        yield 'large number' => [1000000];
    }

    public function test_collect_clears_aggregated_values(): void
    {
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen(),
        );
        $counter = $meter->createCounter('requests');
        $counter->add(100);

        $firstCollect = $meter->collect();
        static::assertCount(1, $firstCollect);
        static::assertSame(100, $firstCollect[0]->value);

        $secondCollect = $meter->collect();
        static::assertCount(0, $secondCollect);
    }

    public function test_complete_collects_metrics_and_passes_to_processor(): void
    {
        $processor = new MemoryMetricProcessor(new MemoryExporter());
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            $processor,
            ClockMother::frozen(),
        );

        $counter = $meter->createCounter('requests.total', 'requests', 'Total requests');
        $counter->add(10, ['method' => 'GET']);
        $counter->add(5, ['method' => 'POST']);

        $meter->complete($counter);

        $processedMetrics = $processor->metrics();
        static::assertCount(2, $processedMetrics);

        $getMetrics = array_filter($processedMetrics, static fn($m) => $m->attributes->get('method') === 'GET');
        $postMetrics = array_filter($processedMetrics, static fn($m) => $m->attributes->get('method') === 'POST');

        static::assertCount(1, $getMetrics);
        static::assertCount(1, $postMetrics);
        static::assertSame(10, array_values($getMetrics)[0]->value);
        static::assertSame(5, array_values($postMetrics)[0]->value);
    }

    public function test_complete_removes_instrument_from_cache(): void
    {
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen(),
        );

        $counter1 = $meter->createCounter('requests');
        $counter1->add(100);

        $meter->complete($counter1);

        $counter2 = $meter->createCounter('requests');

        static::assertNotSame($counter1, $counter2);
    }

    public function test_complete_with_instrument_not_created_by_this_meter(): void
    {
        $processorA = new MemoryMetricProcessor(new MemoryExporter());
        $processorB = new MemoryMetricProcessor(new MemoryExporter());

        $meterA = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('meter-a', '1.0.0'),
            $processorA,
            ClockMother::frozen(),
        );
        $meterB = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('meter-b', '1.0.0'),
            $processorB,
            ClockMother::frozen(),
        );

        $counterFromA = $meterA->createCounter('requests');
        $counterFromA->add(50);

        $meterB->complete($counterFromA);

        static::assertCount(1, $processorB->metrics());
        static::assertSame(50, $processorB->metrics()[0]->value);

        $counterFromB = $meterB->createCounter('requests');
        static::assertNotSame($counterFromA, $counterFromB);
    }

    #[DataProvider('validCounterAmountProvider')]
    public function test_counter_accepts_non_negative_amounts(int|float $amount): void
    {
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen(),
        );
        $counter = $meter->createCounter('counter');
        $counter->add($amount);

        $metrics = $meter->collect();
        static::assertCount(1, $metrics);
        static::assertSame($amount, $metrics[0]->value);
    }

    public function test_counter_aggregates_values_and_produces_metric_on_collect(): void
    {
        $timestamp = new DateTimeImmutable('2024-01-15 10:30:00');
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen($timestamp),
        );

        $counter = $meter->createCounter('requests.total', 'requests', 'Total requests');
        $counter->add(3, ['http.method' => 'POST']);
        $counter->add(2, ['http.method' => 'POST']);

        $metrics = $meter->collect();

        static::assertCount(1, $metrics);
        $metric = $metrics[0];

        static::assertSame('requests.total', $metric->name);
        static::assertSame(MetricType::COUNTER, $metric->type);
        static::assertSame(5, $metric->value);
        static::assertSame(['http.method' => 'POST'], $metric->attributes->normalize());
        static::assertSame('requests', $metric->unit);
        static::assertSame('Total requests', $metric->description);
        static::assertSame($timestamp, $metric->timestamp);
    }

    #[DataProvider('negativeAmountProvider')]
    public function test_counter_rejects_negative_amounts(int|float $amount): void
    {
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen(),
        );
        $counter = $meter->createCounter('counter');

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Counter amount must be >= 0');

        $counter->add($amount);
    }

    public function test_counter_separates_by_attributes(): void
    {
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen(),
        );
        $counter = $meter->createCounter('requests');

        $counter->add(10, ['method' => 'GET']);
        $counter->add(5, ['method' => 'POST']);
        $counter->add(3, ['method' => 'GET']);

        $metrics = $meter->collect();
        static::assertCount(2, $metrics);

        $getMetrics = array_filter($metrics, static fn($m) => $m->attributes->get('method') === 'GET');
        $postMetrics = array_filter($metrics, static fn($m) => $m->attributes->get('method') === 'POST');

        static::assertCount(1, $getMetrics);
        static::assertCount(1, $postMetrics);
        static::assertSame(13, array_values($getMetrics)[0]->value);
        static::assertSame(5, array_values($postMetrics)[0]->value);
    }

    public function test_create_counter_returns_same_instance_for_same_name(): void
    {
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen(),
        );

        $counter1 = $meter->createCounter('requests');
        $counter2 = $meter->createCounter('requests');

        static::assertSame($counter1, $counter2);
    }

    #[DataProvider('gaugeValueProvider')]
    public function test_gauge_accepts_any_value(int|float $value): void
    {
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen(),
        );
        $gauge = $meter->createGauge('gauge');
        $gauge->record($value);

        $metrics = $meter->collect();
        static::assertCount(1, $metrics);
        static::assertSame($value, $metrics[0]->value);
    }

    public function test_gauge_keeps_last_value_on_collect(): void
    {
        $timestamp = new DateTimeImmutable('2024-01-15 10:30:00');
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen($timestamp),
        );

        $gauge = $meter->createGauge('cpu.usage', '%', 'CPU utilization');
        $gauge->record(50.0, ['host' => 'server-1']);
        $gauge->record(75.5, ['host' => 'server-1']);

        $metrics = $meter->collect();

        static::assertCount(1, $metrics);
        $metric = $metrics[0];

        static::assertSame('cpu.usage', $metric->name);
        static::assertSame(MetricType::GAUGE, $metric->type);
        static::assertSame(75.5, $metric->value);
        static::assertSame(['host' => 'server-1'], $metric->attributes->normalize());
        static::assertSame('%', $metric->unit);
        static::assertSame('CPU utilization', $metric->description);
    }

    #[DataProvider('histogramValueProvider')]
    public function test_histogram_accepts_any_value(int|float $value): void
    {
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen(),
        );
        $histogram = $meter->createHistogram('histogram');
        $histogram->record($value);

        $metrics = $meter->collect();
        static::assertCount(1, $metrics);
    }

    public function test_histogram_tracks_distribution_statistics(): void
    {
        $timestamp = new DateTimeImmutable('2024-01-15 10:30:00');
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen($timestamp),
        );

        $histogram = $meter->createHistogram('request.duration', 'ms', 'Request duration');
        $histogram->record(100, ['http.status' => 200]);
        $histogram->record(200, ['http.status' => 200]);
        $histogram->record(150, ['http.status' => 200]);

        $metrics = $meter->collect();

        static::assertCount(1, $metrics);
        $metric = $metrics[0];

        static::assertSame('request.duration', $metric->name);
        static::assertSame(MetricType::HISTOGRAM, $metric->type);
        static::assertSame(450.0, $metric->value);
        static::assertSame('ms', $metric->unit);
        static::assertSame('Request duration', $metric->description);

        static::assertSame(3, $metric->attributes->get('histogram.count'));
        static::assertSame(450.0, $metric->attributes->get('histogram.sum'));
        static::assertSame(100.0, $metric->attributes->get('histogram.min'));
        static::assertSame(200.0, $metric->attributes->get('histogram.max'));
    }

    public function test_meter_exposes_name_and_version(): void
    {
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('my-service', '2.1.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen(),
        );

        static::assertSame('my-service', $meter->name());
        static::assertSame('2.1.0', $meter->version());
    }

    public function test_meter_returns_correct_instrument_types(): void
    {
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen(),
        );

        static::assertInstanceOf(Counter::class, $meter->createCounter('counter'));
        static::assertInstanceOf(UpDownCounter::class, $meter->createUpDownCounter('updown'));
        static::assertInstanceOf(Gauge::class, $meter->createGauge('gauge'));
        static::assertInstanceOf(Histogram::class, $meter->createHistogram('histogram'));
    }

    #[DataProvider('upDownCounterValueProvider')]
    public function test_up_down_counter_accepts_positive_and_negative(int|float $value): void
    {
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen(),
        );
        $counter = $meter->createUpDownCounter('up_down');
        $counter->add($value);

        $metrics = $meter->collect();
        static::assertCount(1, $metrics);
        static::assertSame($value, $metrics[0]->value);
    }

    public function test_up_down_counter_aggregates_values(): void
    {
        $timestamp = new DateTimeImmutable('2024-01-15 10:30:00');
        $meter = new Meter(
            ResourceMother::default(),
            new InstrumentationScope('test-meter', '1.0.0'),
            new VoidMetricProcessor(),
            ClockMother::frozen($timestamp),
        );

        $counter = $meter->createUpDownCounter('queue.size', 'items', 'Queue size');
        $counter->add(10, ['queue' => 'tasks']);
        $counter->add(-3, ['queue' => 'tasks']);
        $counter->add(5, ['queue' => 'tasks']);

        $metrics = $meter->collect();

        static::assertCount(1, $metrics);
        $metric = $metrics[0];

        static::assertSame('queue.size', $metric->name);
        static::assertSame(MetricType::UP_DOWN_COUNTER, $metric->type);
        static::assertSame(12, $metric->value);
        static::assertSame(['queue' => 'tasks'], $metric->attributes->normalize());
        static::assertSame('items', $metric->unit);
        static::assertSame('Queue size', $metric->description);
    }
}
