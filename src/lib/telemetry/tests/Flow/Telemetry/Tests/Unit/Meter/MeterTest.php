<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter;

use Flow\Telemetry\InstrumentationScope;
use Flow\Telemetry\Meter\Instrument\{Counter, Gauge, Histogram, UpDownCounter};
use Flow\Telemetry\Meter\{Meter, MetricType};
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Tests\Mother\{ClockMother, ResourceMother};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class MeterTest extends TestCase
{
    public static function gaugeValueProvider() : \Generator
    {
        yield 'zero' => [0];
        yield 'positive' => [100];
        yield 'negative' => [-50];
        yield 'float' => [3.14159];
    }

    public static function histogramValueProvider() : \Generator
    {
        yield 'zero' => [0];
        yield 'small' => [0.001];
        yield 'medium' => [500];
        yield 'large' => [10000.5];
    }

    public static function negativeAmountProvider() : \Generator
    {
        yield 'negative integer' => [-1];
        yield 'negative float' => [-0.5];
        yield 'large negative' => [-1000];
    }

    public static function upDownCounterValueProvider() : \Generator
    {
        yield 'positive' => [10];
        yield 'negative' => [-10];
        yield 'zero' => [0];
        yield 'positive float' => [5.5];
        yield 'negative float' => [-5.5];
    }

    public static function validCounterAmountProvider() : \Generator
    {
        yield 'zero' => [0];
        yield 'positive integer' => [42];
        yield 'positive float' => [3.14];
        yield 'large number' => [1000000];
    }

    public function test_collect_clears_aggregated_values() : void
    {
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen());
        $counter = $meter->createCounter('requests');
        $counter->add(100);

        $firstCollect = $meter->collect();
        self::assertCount(1, $firstCollect);
        self::assertSame(100, $firstCollect[0]->value);

        $secondCollect = $meter->collect();
        self::assertCount(0, $secondCollect);
    }

    #[DataProvider('validCounterAmountProvider')]
    public function test_counter_accepts_non_negative_amounts(int|float $amount) : void
    {
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen());
        $counter = $meter->createCounter('counter');
        $counter->add($amount);

        $metrics = $meter->collect();
        self::assertCount(1, $metrics);
        self::assertSame($amount, $metrics[0]->value);
    }

    public function test_counter_aggregates_values_and_produces_metric_on_collect() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen($timestamp));

        $counter = $meter->createCounter('requests.total', 'requests', 'Total requests');
        $counter->add(3, ['http.method' => 'POST']);
        $counter->add(2, ['http.method' => 'POST']);

        $metrics = $meter->collect();

        self::assertCount(1, $metrics);
        $metric = $metrics[0];

        self::assertSame('requests.total', $metric->name);
        self::assertSame(MetricType::COUNTER, $metric->type);
        self::assertSame(5, $metric->value);
        self::assertSame(['http.method' => 'POST'], $metric->attributes->normalize());
        self::assertSame('requests', $metric->unit);
        self::assertSame('Total requests', $metric->description);
        self::assertSame($timestamp, $metric->timestamp);
    }

    #[DataProvider('negativeAmountProvider')]
    public function test_counter_rejects_negative_amounts(int|float $amount) : void
    {
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen());
        $counter = $meter->createCounter('counter');

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Counter amount must be >= 0');

        $counter->add($amount);
    }

    public function test_counter_separates_by_attributes() : void
    {
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen());
        $counter = $meter->createCounter('requests');

        $counter->add(10, ['method' => 'GET']);
        $counter->add(5, ['method' => 'POST']);
        $counter->add(3, ['method' => 'GET']);

        $metrics = $meter->collect();
        self::assertCount(2, $metrics);

        $getMetrics = \array_filter($metrics, static fn ($m) => $m->attributes->get('method') === 'GET');
        $postMetrics = \array_filter($metrics, static fn ($m) => $m->attributes->get('method') === 'POST');

        self::assertCount(1, $getMetrics);
        self::assertCount(1, $postMetrics);
        self::assertSame(13, \array_values($getMetrics)[0]->value);
        self::assertSame(5, \array_values($postMetrics)[0]->value);
    }

    public function test_create_counter_returns_same_instance_for_same_name() : void
    {
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen());

        $counter1 = $meter->createCounter('requests');
        $counter2 = $meter->createCounter('requests');

        self::assertSame($counter1, $counter2);
    }

    #[DataProvider('gaugeValueProvider')]
    public function test_gauge_accepts_any_value(int|float $value) : void
    {
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen());
        $gauge = $meter->createGauge('gauge');
        $gauge->record($value);

        $metrics = $meter->collect();
        self::assertCount(1, $metrics);
        self::assertSame($value, $metrics[0]->value);
    }

    public function test_gauge_keeps_last_value_on_collect() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen($timestamp));

        $gauge = $meter->createGauge('cpu.usage', '%', 'CPU utilization');
        $gauge->record(50.0, ['host' => 'server-1']);
        $gauge->record(75.5, ['host' => 'server-1']);

        $metrics = $meter->collect();

        self::assertCount(1, $metrics);
        $metric = $metrics[0];

        self::assertSame('cpu.usage', $metric->name);
        self::assertSame(MetricType::GAUGE, $metric->type);
        self::assertSame(75.5, $metric->value);
        self::assertSame(['host' => 'server-1'], $metric->attributes->normalize());
        self::assertSame('%', $metric->unit);
        self::assertSame('CPU utilization', $metric->description);
    }

    #[DataProvider('histogramValueProvider')]
    public function test_histogram_accepts_any_value(int|float $value) : void
    {
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen());
        $histogram = $meter->createHistogram('histogram');
        $histogram->record($value);

        $metrics = $meter->collect();
        self::assertCount(1, $metrics);
    }

    public function test_histogram_tracks_distribution_statistics() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen($timestamp));

        $histogram = $meter->createHistogram('request.duration', 'ms', 'Request duration');
        $histogram->record(100, ['http.status' => 200]);
        $histogram->record(200, ['http.status' => 200]);
        $histogram->record(150, ['http.status' => 200]);

        $metrics = $meter->collect();

        self::assertCount(1, $metrics);
        $metric = $metrics[0];

        self::assertSame('request.duration', $metric->name);
        self::assertSame(MetricType::HISTOGRAM, $metric->type);
        self::assertSame(450.0, $metric->value);
        self::assertSame('ms', $metric->unit);
        self::assertSame('Request duration', $metric->description);

        self::assertSame(3, $metric->attributes->get('histogram.count'));
        self::assertSame(450.0, $metric->attributes->get('histogram.sum'));
        self::assertSame(100.0, $metric->attributes->get('histogram.min'));
        self::assertSame(200.0, $metric->attributes->get('histogram.max'));
    }

    public function test_meter_exposes_name_and_version() : void
    {
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('my-service', '2.1.0'), new VoidMetricProcessor(), ClockMother::frozen());

        self::assertSame('my-service', $meter->name());
        self::assertSame('2.1.0', $meter->version());
    }

    public function test_meter_returns_correct_instrument_types() : void
    {
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen());

        self::assertInstanceOf(Counter::class, $meter->createCounter('counter'));
        self::assertInstanceOf(UpDownCounter::class, $meter->createUpDownCounter('updown'));
        self::assertInstanceOf(Gauge::class, $meter->createGauge('gauge'));
        self::assertInstanceOf(Histogram::class, $meter->createHistogram('histogram'));
    }

    #[DataProvider('upDownCounterValueProvider')]
    public function test_up_down_counter_accepts_positive_and_negative(int|float $value) : void
    {
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen());
        $counter = $meter->createUpDownCounter('up_down');
        $counter->add($value);

        $metrics = $meter->collect();
        self::assertCount(1, $metrics);
        self::assertSame($value, $metrics[0]->value);
    }

    public function test_up_down_counter_aggregates_values() : void
    {
        $timestamp = new \DateTimeImmutable('2024-01-15 10:30:00');
        $meter = new Meter(ResourceMother::default(), new InstrumentationScope('test-meter', '1.0.0'), new VoidMetricProcessor(), ClockMother::frozen($timestamp));

        $counter = $meter->createUpDownCounter('queue.size', 'items', 'Queue size');
        $counter->add(10, ['queue' => 'tasks']);
        $counter->add(-3, ['queue' => 'tasks']);
        $counter->add(5, ['queue' => 'tasks']);

        $metrics = $meter->collect();

        self::assertCount(1, $metrics);
        $metric = $metrics[0];

        self::assertSame('queue.size', $metric->name);
        self::assertSame(MetricType::UP_DOWN_COUNTER, $metric->type);
        self::assertSame(12, $metric->value);
        self::assertSame(['queue' => 'tasks'], $metric->attributes->normalize());
        self::assertSame('items', $metric->unit);
        self::assertSame('Queue size', $metric->description);
    }
}
