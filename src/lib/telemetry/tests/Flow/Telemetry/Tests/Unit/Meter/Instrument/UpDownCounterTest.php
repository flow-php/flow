<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Instrument;

use Flow\Telemetry\Meter\Instrument\UpDownCounter;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Tests\Mother\{ClockMother, InstrumentationScopeMother, ResourceMother};
use PHPUnit\Framework\TestCase;

final class UpDownCounterTest extends TestCase
{
    public function test_add_multiple_values_aggregates() : void
    {
        $counter = new UpDownCounter(
            'test.updowncounter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(10);
        $counter->add(5);
        $counter->add(-3);

        $metrics = $counter->collect();

        self::assertCount(1, $metrics);
        self::assertSame(12, $metrics[0]->value);
    }

    public function test_add_negative_value() : void
    {
        $counter = new UpDownCounter(
            'test.updowncounter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(100);
        $counter->add(-30);

        $metrics = $counter->collect();

        self::assertCount(1, $metrics);
        self::assertSame(70, $metrics[0]->value);
    }

    public function test_add_positive_value() : void
    {
        $counter = new UpDownCounter(
            'test.updowncounter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(42);

        $metrics = $counter->collect();

        self::assertCount(1, $metrics);
        self::assertSame(42, $metrics[0]->value);
    }

    public function test_collect_resets_counter() : void
    {
        $counter = new UpDownCounter(
            'test.updowncounter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(100);
        $counter->collect();

        $counter->add(50);
        $metrics = $counter->collect();

        self::assertCount(1, $metrics);
        self::assertSame(50, $metrics[0]->value);
    }

    public function test_different_attributes_create_separate_aggregations() : void
    {
        $counter = new UpDownCounter(
            'test.updowncounter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(10, ['queue' => 'tasks']);
        $counter->add(-5, ['queue' => 'tasks']);
        $counter->add(3, ['queue' => 'jobs']);

        $metrics = $counter->collect();

        self::assertCount(2, $metrics);

        $tasksMetrics = \array_values(\array_filter($metrics, static fn ($m) => $m->attributes->get('queue') === 'tasks'));
        $jobsMetrics = \array_values(\array_filter($metrics, static fn ($m) => $m->attributes->get('queue') === 'jobs'));

        self::assertCount(1, $tasksMetrics);
        self::assertCount(1, $jobsMetrics);
        self::assertSame(5, $tasksMetrics[0]->value);
        self::assertSame(3, $jobsMetrics[0]->value);
    }

    public function test_up_down_counter_returns_correct_metric_type() : void
    {
        $counter = new UpDownCounter(
            'test.updowncounter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(1);
        $metrics = $counter->collect();

        self::assertSame(MetricType::UP_DOWN_COUNTER, $metrics[0]->type);
    }

    public function test_up_down_counter_starts_at_zero() : void
    {
        $counter = new UpDownCounter(
            'test.updowncounter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $metrics = $counter->collect();

        self::assertCount(0, $metrics);
    }
}
