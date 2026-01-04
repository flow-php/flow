<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Void;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Logger\LogExporter;
use Flow\Telemetry\Meter\{Metric, MetricExporter, MetricType};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidMetricExporter, VoidSpanExporter};
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, ResourceMother, SpanMother};
use Flow\Telemetry\Tracer\SpanExporter;
use PHPUnit\Framework\TestCase;

final class VoidExporterTest extends TestCase
{
    public function test_export_logs_returns_true() : void
    {
        self::assertTrue((new VoidLogExporter())->export([]));
    }

    public function test_export_logs_with_empty_returns_true() : void
    {
        self::assertTrue((new VoidLogExporter())->export([]));
    }

    public function test_export_metrics_returns_true() : void
    {
        $metrics = [
            new Metric(
                name: 'test.metric',
                type: MetricType::COUNTER,
                value: 1,
                attributes: Attributes::empty(),
                timestamp: new \DateTimeImmutable(),
                resource: ResourceMother::default(),
                scope: InstrumentationScopeMother::default(),
            ),
        ];

        self::assertTrue((new VoidMetricExporter())->export($metrics));
    }

    public function test_export_metrics_with_empty_returns_true() : void
    {
        self::assertTrue((new VoidMetricExporter())->export([]));
    }

    public function test_export_spans_returns_true() : void
    {
        self::assertTrue((new VoidSpanExporter())->export([
            SpanMother::withName('span-1'),
            SpanMother::withName('span-2'),
        ]));
    }

    public function test_export_spans_with_empty_returns_true() : void
    {
        self::assertTrue((new VoidSpanExporter())->export([]));
    }

    public function test_void_log_exporter_implements_log_exporter() : void
    {
        self::assertInstanceOf(LogExporter::class, new VoidLogExporter());
    }

    public function test_void_metric_exporter_implements_metric_exporter() : void
    {
        self::assertInstanceOf(MetricExporter::class, new VoidMetricExporter());
    }

    public function test_void_span_exporter_implements_span_exporter() : void
    {
        self::assertInstanceOf(SpanExporter::class, new VoidSpanExporter());
    }
}
