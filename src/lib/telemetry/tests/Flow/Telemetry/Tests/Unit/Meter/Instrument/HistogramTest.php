<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Instrument;

use Flow\Telemetry\Meter\{AggregationTemporality, MetricType};
use Flow\Telemetry\Meter\Instrument\Histogram;
use Flow\Telemetry\Tests\Mother\{ClockMother, InstrumentationScopeMother, ResourceMother, SpanContextMother};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class HistogramTest extends TestCase
{
    public static function bucketBoundaryTestCases() : \Generator
    {
        yield 'value at exact boundary falls into that bucket' => [
            'value' => 50.0,
            'boundaries' => [10.0, 50.0, 100.0],
            'expectedBucketIndex' => 1,
        ];

        yield 'value below first boundary' => [
            'value' => 5.0,
            'boundaries' => [10.0, 50.0, 100.0],
            'expectedBucketIndex' => 0,
        ];

        yield 'value above last boundary (overflow bucket)' => [
            'value' => 150.0,
            'boundaries' => [10.0, 50.0, 100.0],
            'expectedBucketIndex' => 3,
        ];

        yield 'value between boundaries' => [
            'value' => 75.0,
            'boundaries' => [10.0, 50.0, 100.0],
            'expectedBucketIndex' => 2,
        ];

        yield 'negative value with positive boundaries' => [
            'value' => -5.0,
            'boundaries' => [0.0, 10.0, 50.0],
            'expectedBucketIndex' => 0,
        ];

        yield 'zero value with zero first boundary' => [
            'value' => 0.0,
            'boundaries' => [0.0, 10.0, 50.0],
            'expectedBucketIndex' => 0,
        ];
    }

    public function test_boundaries_getter_returns_configured_boundaries() : void
    {
        $customBoundaries = [1.0, 5.0, 10.0, 25.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $customBoundaries,
        );

        self::assertSame($customBoundaries, $histogram->boundaries());
    }

    public function test_bucket_counts_are_included_in_collected_metric() : void
    {
        $boundaries = [10.0, 50.0, 100.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $boundaries,
        );

        $histogram->record(5.0);
        $histogram->record(25.0);
        $histogram->record(75.0);
        $histogram->record(150.0);

        $metrics = $histogram->collect();

        self::assertCount(1, $metrics);
        $metric = $metrics[0];

        self::assertTrue($metric->attributes->has('histogram.bucketCounts'));
        self::assertTrue($metric->attributes->has('histogram.explicitBounds'));
        self::assertSame($boundaries, $metric->attributes->get('histogram.explicitBounds'));
        self::assertSame([1, 1, 1, 1], $metric->attributes->get('histogram.bucketCounts'));
    }

    public function test_bucket_counts_sum_equals_total_count() : void
    {
        $boundaries = [10.0, 50.0, 100.0, 500.0, 1000.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $boundaries,
        );

        $histogram->record(5.0);
        $histogram->record(15.0);
        $histogram->record(25.0);
        $histogram->record(75.0);
        $histogram->record(200.0);
        $histogram->record(750.0);
        $histogram->record(5000.0);
        $histogram->record(5000.0);
        $histogram->record(5000.0);

        $metrics = $histogram->collect();
        $metric = $metrics[0];

        /** @var array<int, int> $bucketCounts */
        $bucketCounts = $metric->attributes->get('histogram.bucketCounts');
        $totalFromBuckets = \array_sum($bucketCounts);
        $totalCount = $metric->attributes->get('histogram.count');

        self::assertSame($totalCount, $totalFromBuckets);
        self::assertSame(9, $totalCount);
    }

    public function test_custom_boundaries_can_be_configured() : void
    {
        $customBoundaries = [1.0, 2.0, 5.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $customBoundaries,
        );

        self::assertSame($customBoundaries, $histogram->boundaries());
    }

    public function test_default_boundaries_are_set() : void
    {
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $expectedBoundaries = [0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0];
        self::assertSame($expectedBoundaries, $histogram->boundaries());
    }

    public function test_empty_boundaries_produces_single_bucket() : void
    {
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: [],
        );

        $histogram->record(50.0);
        $histogram->record(100.0);
        $histogram->record(1000.0);

        $metrics = $histogram->collect();
        $metric = $metrics[0];

        self::assertSame([], $metric->attributes->get('histogram.explicitBounds'));
        self::assertSame([3], $metric->attributes->get('histogram.bucketCounts'));
    }

    public function test_empty_histogram_has_zero_bucket_counts() : void
    {
        $boundaries = [10.0, 50.0, 100.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $boundaries,
        );

        $metrics = $histogram->collect();

        self::assertCount(0, $metrics);
    }

    public function test_exemplar_captured_per_bucket() : void
    {
        $boundaries = [10.0, 50.0, 100.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $boundaries,
        );

        $spanContext1 = SpanContextMother::create();
        $spanContext2 = SpanContextMother::create();
        $spanContext3 = SpanContextMother::create();

        $histogram->record(5.0, [], $spanContext1);
        $histogram->record(75.0, [], $spanContext2);
        $histogram->record(150.0, [], $spanContext3);

        $metrics = $histogram->collect();

        self::assertCount(1, $metrics);
        self::assertCount(3, $metrics[0]->exemplars);

        $values = \array_map(fn ($e) => $e->value, $metrics[0]->exemplars);
        self::assertContains(5.0, $values);
        self::assertContains(75.0, $values);
        self::assertContains(150.0, $values);
    }

    public function test_exemplar_not_captured_without_span_context() : void
    {
        $boundaries = [10.0, 50.0, 100.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $boundaries,
        );

        $histogram->record(25.0);

        $metrics = $histogram->collect();

        self::assertCount(1, $metrics);
        self::assertCount(0, $metrics[0]->exemplars);
    }

    public function test_exemplar_replaced_in_same_bucket() : void
    {
        $boundaries = [10.0, 50.0, 100.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $boundaries,
        );

        $spanContext1 = SpanContextMother::create();
        $spanContext2 = SpanContextMother::create();

        $histogram->record(5.0, [], $spanContext1);
        $histogram->record(8.0, [], $spanContext2);

        $metrics = $histogram->collect();

        self::assertCount(1, $metrics);
        self::assertCount(1, $metrics[0]->exemplars);
        self::assertSame(8.0, $metrics[0]->exemplars[0]->value);
        self::assertSame($spanContext2->spanId->toHex(), $metrics[0]->exemplars[0]->spanId->toHex());
    }

    public function test_histogram_preserves_aggregate_statistics_with_buckets() : void
    {
        $boundaries = [10.0, 50.0, 100.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            unit: 'ms',
            description: 'Test histogram',
            boundaries: $boundaries,
        );

        $histogram->record(5.0);
        $histogram->record(25.0);
        $histogram->record(75.0);
        $histogram->record(150.0);

        $metrics = $histogram->collect();
        $metric = $metrics[0];

        self::assertSame('test.histogram', $metric->name);
        self::assertSame(MetricType::HISTOGRAM, $metric->type);
        self::assertSame(255.0, $metric->value);
        self::assertSame('ms', $metric->unit);
        self::assertSame('Test histogram', $metric->description);
        self::assertSame(4, $metric->attributes->get('histogram.count'));
        self::assertSame(255.0, $metric->attributes->get('histogram.sum'));
        self::assertSame(5.0, $metric->attributes->get('histogram.min'));
        self::assertSame(150.0, $metric->attributes->get('histogram.max'));
    }

    public function test_multiple_attribute_sets_have_separate_bucket_counts() : void
    {
        $boundaries = [10.0, 50.0, 100.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $boundaries,
        );

        $histogram->record(5.0, ['method' => 'GET']);
        $histogram->record(75.0, ['method' => 'GET']);
        $histogram->record(25.0, ['method' => 'POST']);

        $metrics = $histogram->collect();

        self::assertCount(2, $metrics);

        $getMetrics = \array_values(\array_filter($metrics, fn ($m) => $m->attributes->get('method') === 'GET'));
        $postMetrics = \array_values(\array_filter($metrics, fn ($m) => $m->attributes->get('method') === 'POST'));

        self::assertCount(1, $getMetrics);
        self::assertCount(1, $postMetrics);

        self::assertSame([1, 0, 1, 0], $getMetrics[0]->attributes->get('histogram.bucketCounts'));
        self::assertSame([0, 1, 0, 0], $postMetrics[0]->attributes->get('histogram.bucketCounts'));
    }

    public function test_overflow_bucket_captures_large_values() : void
    {
        $boundaries = [10.0, 50.0, 100.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $boundaries,
        );

        $histogram->record(500.0);
        $histogram->record(1000.0);
        $histogram->record(10000.0);

        $metrics = $histogram->collect();
        $metric = $metrics[0];

        /** @var array<int, int> $bucketCounts */
        $bucketCounts = $metric->attributes->get('histogram.bucketCounts');
        self::assertCount(4, $bucketCounts);
        self::assertSame(3, $bucketCounts[3]);
    }

    public function test_underflow_bucket_captures_small_values() : void
    {
        $boundaries = [10.0, 50.0, 100.0];
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $boundaries,
        );

        $histogram->record(0.001);
        $histogram->record(5.0);
        $histogram->record(9.999);

        $metrics = $histogram->collect();
        $metric = $metrics[0];

        /** @var array<int, int> $bucketCounts */
        $bucketCounts = $metric->attributes->get('histogram.bucketCounts');
        self::assertCount(4, $bucketCounts);
        self::assertSame(3, $bucketCounts[0]);
    }

    /**
     * @param array<float> $boundaries
     */
    #[DataProvider('bucketBoundaryTestCases')]
    public function test_values_are_placed_in_correct_buckets(float $value, array $boundaries, int $expectedBucketIndex) : void
    {
        $histogram = new Histogram(
            'test.histogram',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            temporality: AggregationTemporality::CUMULATIVE,
            boundaries: $boundaries,
        );

        $histogram->record($value);

        $metrics = $histogram->collect();
        /** @var array<int, int> $bucketCounts */
        $bucketCounts = $metrics[0]->attributes->get('histogram.bucketCounts');

        for ($i = 0; $i < \count($bucketCounts); $i++) {
            if ($i === $expectedBucketIndex) {
                self::assertSame(1, $bucketCounts[$i], "Expected bucket {$i} to have count 1");
            } else {
                self::assertSame(0, $bucketCounts[$i], "Expected bucket {$i} to have count 0");
            }
        }
    }
}
