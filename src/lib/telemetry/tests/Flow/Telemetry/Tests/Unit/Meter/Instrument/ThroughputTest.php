<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Instrument;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Meter\Instrument\Throughput;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Meter\TimeUnit;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tests\Mother\SpanContextMother;
use PHPUnit\Framework\TestCase;

use function strlen;
use function strrchr;
use function substr;

final class ThroughputTest extends TestCase
{
    public function test_add_accumulates_counts(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $throughput->add(100);
        $throughput->add(150);
        $throughput->add(50);

        $metrics = $throughput->collect();

        static::assertCount(1, $metrics);
        static::assertIsFloat($metrics[0]->value);
    }

    public function test_attributes_object_normalized(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $throughput->add(50, Attributes::create(['source' => 'parquet', 'version' => 2]));
        $metrics = $throughput->collect();

        static::assertCount(1, $metrics);
        static::assertSame('parquet', $metrics[0]->attributes->get('source'));
        static::assertSame(2, $metrics[0]->attributes->get('version'));
    }

    public function test_collect_returns_metric_with_rate(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            ratePrecision: null,
        );

        $throughput->add(1000);

        $metrics = $throughput->collect();

        static::assertCount(1, $metrics);
        static::assertIsFloat($metrics[0]->value);
        static::assertGreaterThan(0, $metrics[0]->value);
    }

    public function test_creates_instance_with_metadata(): void
    {
        $throughput = new Throughput(
            'dataframe_throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            unit: 'rows',
            description: 'Rows processed per second',
        );

        static::assertSame('dataframe_throughput', $throughput->name());
        static::assertSame('rows/sec', $throughput->unit());
        static::assertSame('Rows processed per second', $throughput->description());
    }

    public function test_custom_attributes_included_in_metric(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $throughput->add(100, ['source' => 'csv', 'pipeline' => 'main']);
        $metrics = $throughput->collect();

        static::assertCount(1, $metrics);
        static::assertSame('csv', $metrics[0]->attributes->get('source'));
        static::assertSame('main', $metrics[0]->attributes->get('pipeline'));
    }

    public function test_custom_attributes_preserved_in_metric(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $throughput->add(100, ['source' => 'csv']);
        $metrics = $throughput->collect();

        static::assertCount(1, $metrics);
        static::assertSame('csv', $metrics[0]->attributes->get('source'));
    }

    public function test_default_time_unit_is_seconds(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            unit: 'rows',
        );

        static::assertSame('rows/sec', $throughput->unit());
    }

    public function test_different_attribute_sets_produce_separate_metrics(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $throughput->add(100, ['source' => 'csv']);
        $throughput->add(200, ['source' => 'parquet']);
        $throughput->add(50, ['source' => 'json']);

        $metrics = $throughput->collect();

        static::assertCount(3, $metrics);

        $metricsBySource = [];

        foreach ($metrics as $metric) {
            $source = $metric->attributes->get('source');
            static::assertIsString($source);
            $metricsBySource[$source] = $metric;
        }

        static::assertArrayHasKey('csv', $metricsBySource);
        static::assertArrayHasKey('parquet', $metricsBySource);
        static::assertArrayHasKey('json', $metricsBySource);
    }

    public function test_exemplar_captured_when_span_context_provided(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $spanContext = SpanContextMother::create();
        $throughput->add(100, ['source' => 'csv'], $spanContext);

        $metrics = $throughput->collect();

        static::assertCount(1, $metrics);
        static::assertCount(1, $metrics[0]->exemplars);
        static::assertSame($spanContext->traceId, $metrics[0]->exemplars[0]->traceId);
        static::assertSame($spanContext->spanId, $metrics[0]->exemplars[0]->spanId);
    }

    public function test_metric_metadata_preserved(): void
    {
        $throughput = new Throughput(
            'dataframe_throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            unit: 'rows',
            description: 'Rows processed per second',
        );

        $throughput->add(100);
        $metrics = $throughput->collect();

        static::assertCount(1, $metrics);
        static::assertSame('dataframe_throughput', $metrics[0]->name);
        static::assertSame('rows/sec', $metrics[0]->unit);
        static::assertSame('Rows processed per second', $metrics[0]->description);
    }

    public function test_no_metrics_when_no_add_calls(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $metrics = $throughput->collect();

        static::assertCount(0, $metrics);
    }

    public function test_no_rounding_when_rate_precision_is_null(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            ratePrecision: null,
        );

        $throughput->add(1000);
        $metrics = $throughput->collect();

        static::assertIsFloat($metrics[0]->value);
    }

    public function test_rate_precision_applied_to_value(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            ratePrecision: 0,
        );

        $throughput->add(1000);
        $metrics = $throughput->collect();

        $rateString = (string) $metrics[0]->value;
        $rateDecimalPlaces = strlen(substr(strrchr($rateString, '.') ?: '', 1));
        static::assertLessThanOrEqual(0, $rateDecimalPlaces);
    }

    public function test_returns_correct_metric_type(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $throughput->add(1);
        $metrics = $throughput->collect();

        static::assertSame(MetricType::GAUGE, $metrics[0]->type);
    }

    public function test_same_attributes_aggregated_together(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $throughput->add(100, ['source' => 'csv']);
        $throughput->add(150, ['source' => 'csv']);
        $throughput->add(50, ['source' => 'csv']);

        $metrics = $throughput->collect();

        static::assertCount(1, $metrics);
        static::assertSame('csv', $metrics[0]->attributes->get('source'));
        static::assertIsFloat($metrics[0]->value);
    }

    public function test_time_unit_affects_unit_string(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            unit: 'rows',
            timeUnit: TimeUnit::MILLISECONDS,
        );

        static::assertSame('rows/ms', $throughput->unit());
    }

    public function test_unit_is_null_when_measurement_unit_is_null(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        static::assertNull($throughput->unit());
    }

    public function test_uses_custom_rate_precision(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            ratePrecision: 4,
        );

        $throughput->add(1000);
        $metrics = $throughput->collect();

        $valueString = (string) $metrics[0]->value;
        $decimalPlaces = strlen(substr(strrchr($valueString, '.') ?: '', 1));

        static::assertLessThanOrEqual(4, $decimalPlaces);
    }

    public function test_uses_custom_time_unit_milliseconds(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            unit: 'bytes',
            timeUnit: TimeUnit::MILLISECONDS,
        );

        static::assertSame('bytes/ms', $throughput->unit());
    }

    public function test_uses_custom_time_unit_minutes(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
            unit: 'requests',
            timeUnit: TimeUnit::MINUTES,
        );

        static::assertSame('requests/min', $throughput->unit());
    }

    public function test_uses_default_precision_of_two_decimal_places(): void
    {
        $throughput = new Throughput(
            'test.throughput',
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
            ClockMother::frozen(),
        );

        $throughput->add(1000);
        $metrics = $throughput->collect();

        $valueString = (string) $metrics[0]->value;
        $decimalPlaces = strlen(substr(strrchr($valueString, '.') ?: '', 1));

        static::assertLessThanOrEqual(2, $decimalPlaces);
    }
}
