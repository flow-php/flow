<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Instrument;

use Flow\Telemetry\Meter\{AggregationTemporality, MetricType};
use Flow\Telemetry\Meter\Instrument\Counter;
use Flow\Telemetry\Tests\Mother\{ClockMother, InstrumentationScopeMother, ResourceMother, SpanContextMother};
use PHPUnit\Framework\TestCase;

final class CounterTest extends TestCase
{
    public function test_add_multiple_values_aggregates() : void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(10);
        $counter->add(5);
        $counter->add(3);

        $metrics = $counter->collect();

        self::assertCount(1, $metrics);
        self::assertSame(18, $metrics[0]->value);
    }

    public function test_add_negative_value_throws_exception() : void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Counter amount must be >= 0, got -5');

        $counter->add(-5);
    }

    public function test_add_positive_value() : void
    {
        $counter = new Counter(
            'test.counter',
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
        $counter = new Counter(
            'test.counter',
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

    public function test_counter_metadata_preserved() : void
    {
        $counter = new Counter(
            'http.requests',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::DELTA,
            unit: 'requests',
            description: 'Total HTTP requests',
        );

        $counter->add(1);
        $metrics = $counter->collect();

        self::assertCount(1, $metrics);
        self::assertSame('http.requests', $metrics[0]->name);
        self::assertSame('requests', $metrics[0]->unit);
        self::assertSame('Total HTTP requests', $metrics[0]->description);
        self::assertSame(AggregationTemporality::DELTA, $metrics[0]->temporality);
    }

    public function test_counter_returns_correct_metric_type() : void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(1);
        $metrics = $counter->collect();

        self::assertSame(MetricType::COUNTER, $metrics[0]->type);
    }

    public function test_counter_starts_at_zero() : void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $metrics = $counter->collect();

        self::assertCount(0, $metrics);
    }

    public function test_different_attributes_create_separate_aggregations() : void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(10, ['method' => 'GET']);
        $counter->add(5, ['method' => 'GET']);
        $counter->add(3, ['method' => 'POST']);

        $metrics = $counter->collect();

        self::assertCount(2, $metrics);

        $getMetrics = \array_values(\array_filter($metrics, fn ($m) => $m->attributes->get('method') === 'GET'));
        $postMetrics = \array_values(\array_filter($metrics, fn ($m) => $m->attributes->get('method') === 'POST'));

        self::assertCount(1, $getMetrics);
        self::assertCount(1, $postMetrics);
        self::assertSame(15, $getMetrics[0]->value);
        self::assertSame(3, $postMetrics[0]->value);
    }

    public function test_exemplar_captured_when_span_context_provided() : void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $spanContext = SpanContextMother::withFixedIds();

        $counter->add(42, ['method' => 'GET'], $spanContext);

        $metrics = $counter->collect();

        self::assertCount(1, $metrics);
        self::assertCount(1, $metrics[0]->exemplars);

        $exemplar = $metrics[0]->exemplars[0];
        self::assertSame(42, $exemplar->value);
        self::assertSame($spanContext->traceId->toHex(), $exemplar->traceId->toHex());
        self::assertSame($spanContext->spanId->toHex(), $exemplar->spanId->toHex());
        self::assertSame(['method' => 'GET'], $exemplar->filteredAttributes);
    }

    public function test_exemplar_not_captured_without_span_context() : void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(42);

        $metrics = $counter->collect();

        self::assertCount(1, $metrics);
        self::assertCount(0, $metrics[0]->exemplars);
    }

    public function test_latest_exemplar_replaces_previous() : void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $spanContext1 = SpanContextMother::create();
        $spanContext2 = SpanContextMother::create();

        $counter->add(10, [], $spanContext1);
        $counter->add(20, [], $spanContext2);

        $metrics = $counter->collect();

        self::assertCount(1, $metrics);
        self::assertCount(1, $metrics[0]->exemplars);
        self::assertSame(20, $metrics[0]->exemplars[0]->value);
        self::assertSame($spanContext2->traceId->toHex(), $metrics[0]->exemplars[0]->traceId->toHex());
    }
}
