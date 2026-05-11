<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Instrument;

use Flow\Telemetry\Meter\AggregationTemporality;
use Flow\Telemetry\Meter\Instrument\Counter;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tests\Mother\SpanContextMother;
use PHPUnit\Framework\TestCase;

final class CounterTest extends TestCase
{
    public function test_add_multiple_values_aggregates(): void
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

        static::assertCount(1, $metrics);
        static::assertSame(18, $metrics[0]->value);
    }

    public function test_add_negative_value_throws_exception(): void
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

    public function test_add_positive_value(): void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(42);

        $metrics = $counter->collect();

        static::assertCount(1, $metrics);
        static::assertSame(42, $metrics[0]->value);
    }

    public function test_collect_resets_counter(): void
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

        static::assertCount(1, $metrics);
        static::assertSame(50, $metrics[0]->value);
    }

    public function test_counter_metadata_preserved(): void
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

        static::assertCount(1, $metrics);
        static::assertSame('http.requests', $metrics[0]->name);
        static::assertSame('requests', $metrics[0]->unit);
        static::assertSame('Total HTTP requests', $metrics[0]->description);
        static::assertSame(AggregationTemporality::DELTA, $metrics[0]->temporality);
    }

    public function test_counter_returns_correct_metric_type(): void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(1);
        $metrics = $counter->collect();

        static::assertSame(MetricType::COUNTER, $metrics[0]->type);
    }

    public function test_counter_starts_at_zero(): void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $metrics = $counter->collect();

        static::assertCount(0, $metrics);
    }

    public function test_different_attributes_create_separate_aggregations(): void
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

        static::assertCount(2, $metrics);

        $getMetrics = \array_values(\array_filter($metrics, static fn($m) => $m->attributes->get('method') === 'GET'));
        $postMetrics = \array_values(\array_filter(
            $metrics,
            static fn($m) => $m->attributes->get('method') === 'POST',
        ));

        static::assertCount(1, $getMetrics);
        static::assertCount(1, $postMetrics);
        static::assertSame(15, $getMetrics[0]->value);
        static::assertSame(3, $postMetrics[0]->value);
    }

    public function test_exemplar_captured_when_span_context_provided(): void
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

        static::assertCount(1, $metrics);
        static::assertCount(1, $metrics[0]->exemplars);

        $exemplar = $metrics[0]->exemplars[0];
        static::assertSame(42, $exemplar->value);
        static::assertSame($spanContext->traceId->toHex(), $exemplar->traceId->toHex());
        static::assertSame($spanContext->spanId->toHex(), $exemplar->spanId->toHex());
        static::assertSame(['method' => 'GET'], $exemplar->filteredAttributes);
    }

    public function test_exemplar_not_captured_without_span_context(): void
    {
        $counter = new Counter(
            'test.counter',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $counter->add(42);

        $metrics = $counter->collect();

        static::assertCount(1, $metrics);
        static::assertCount(0, $metrics[0]->exemplars);
    }

    public function test_latest_exemplar_replaces_previous(): void
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

        static::assertCount(1, $metrics);
        static::assertCount(1, $metrics[0]->exemplars);
        static::assertSame(20, $metrics[0]->exemplars[0]->value);
        static::assertSame($spanContext2->traceId->toHex(), $metrics[0]->exemplars[0]->traceId->toHex());
    }
}
