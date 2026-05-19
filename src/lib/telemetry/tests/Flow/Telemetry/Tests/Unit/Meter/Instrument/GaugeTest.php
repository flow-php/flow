<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Instrument;

use Flow\Telemetry\Meter\Instrument\Gauge;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

use function array_filter;
use function array_values;

final class GaugeTest extends TestCase
{
    public function test_collect_resets_gauge(): void
    {
        $gauge = new Gauge(
            'test.gauge',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $gauge->record(100);
        $gauge->collect();

        $gauge->record(50);
        $metrics = $gauge->collect();

        static::assertCount(1, $metrics);
        static::assertSame(50, $metrics[0]->value);
    }

    public function test_different_attributes_create_separate_aggregations(): void
    {
        $gauge = new Gauge(
            'test.gauge',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $gauge->record(100, ['host' => 'server-1']);
        $gauge->record(200, ['host' => 'server-2']);

        $metrics = $gauge->collect();

        static::assertCount(2, $metrics);

        $server1Metrics = array_values(array_filter(
            $metrics,
            static fn($m) => $m->attributes->get('host') === 'server-1',
        ));
        $server2Metrics = array_values(array_filter(
            $metrics,
            static fn($m) => $m->attributes->get('host') === 'server-2',
        ));

        static::assertCount(1, $server1Metrics);
        static::assertCount(1, $server2Metrics);
        static::assertSame(100, $server1Metrics[0]->value);
        static::assertSame(200, $server2Metrics[0]->value);
    }

    public function test_gauge_metadata_preserved(): void
    {
        $gauge = new Gauge(
            'system.memory.usage',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            unit: 'bytes',
            description: 'Current memory usage',
        );

        $gauge->record(1024);
        $metrics = $gauge->collect();

        static::assertCount(1, $metrics);
        static::assertSame('system.memory.usage', $metrics[0]->name);
        static::assertSame('bytes', $metrics[0]->unit);
        static::assertSame('Current memory usage', $metrics[0]->description);
    }

    public function test_gauge_returns_correct_metric_type(): void
    {
        $gauge = new Gauge(
            'test.gauge',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $gauge->record(1);
        $metrics = $gauge->collect();

        static::assertSame(MetricType::GAUGE, $metrics[0]->type);
    }

    public function test_gauge_starts_empty(): void
    {
        $gauge = new Gauge(
            'test.gauge',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $metrics = $gauge->collect();

        static::assertCount(0, $metrics);
    }

    public function test_record_replaces_previous_value(): void
    {
        $gauge = new Gauge(
            'test.gauge',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $gauge->record(100);
        $gauge->record(50);
        $gauge->record(75);

        $metrics = $gauge->collect();

        static::assertCount(1, $metrics);
        static::assertSame(75, $metrics[0]->value);
    }

    public function test_record_stores_value(): void
    {
        $gauge = new Gauge(
            'test.gauge',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $gauge->record(42);

        $metrics = $gauge->collect();

        static::assertCount(1, $metrics);
        static::assertSame(42, $metrics[0]->value);
    }
}
