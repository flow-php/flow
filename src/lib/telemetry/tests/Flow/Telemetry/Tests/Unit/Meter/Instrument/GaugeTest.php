<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Instrument;

use Flow\Telemetry\Meter\Instrument\Gauge;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Tests\Mother\{ClockMother, InstrumentationScopeMother, ResourceMother};
use PHPUnit\Framework\TestCase;

final class GaugeTest extends TestCase
{
    public function test_collect_resets_gauge() : void
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

        self::assertCount(1, $metrics);
        self::assertSame(50, $metrics[0]->value);
    }

    public function test_different_attributes_create_separate_aggregations() : void
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

        self::assertCount(2, $metrics);

        $server1Metrics = \array_values(\array_filter($metrics, static fn ($m) => $m->attributes->get('host') === 'server-1'));
        $server2Metrics = \array_values(\array_filter($metrics, static fn ($m) => $m->attributes->get('host') === 'server-2'));

        self::assertCount(1, $server1Metrics);
        self::assertCount(1, $server2Metrics);
        self::assertSame(100, $server1Metrics[0]->value);
        self::assertSame(200, $server2Metrics[0]->value);
    }

    public function test_gauge_metadata_preserved() : void
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

        self::assertCount(1, $metrics);
        self::assertSame('system.memory.usage', $metrics[0]->name);
        self::assertSame('bytes', $metrics[0]->unit);
        self::assertSame('Current memory usage', $metrics[0]->description);
    }

    public function test_gauge_returns_correct_metric_type() : void
    {
        $gauge = new Gauge(
            'test.gauge',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $gauge->record(1);
        $metrics = $gauge->collect();

        self::assertSame(MetricType::GAUGE, $metrics[0]->type);
    }

    public function test_gauge_starts_empty() : void
    {
        $gauge = new Gauge(
            'test.gauge',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $metrics = $gauge->collect();

        self::assertCount(0, $metrics);
    }

    public function test_record_replaces_previous_value() : void
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

        self::assertCount(1, $metrics);
        self::assertSame(75, $metrics[0]->value);
    }

    public function test_record_stores_value() : void
    {
        $gauge = new Gauge(
            'test.gauge',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $gauge->record(42);

        $metrics = $gauge->collect();

        self::assertCount(1, $metrics);
        self::assertSame(42, $metrics[0]->value);
    }
}
