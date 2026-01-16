<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Memory;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Meter\{Metric, MetricExporter, MetricType};
use Flow\Telemetry\Provider\Memory\MemoryMetricExporter;
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, ResourceMother};
use Flow\Telemetry\Transport\VoidTransport;
use PHPUnit\Framework\TestCase;

final class MemoryMetricExporterTest extends TestCase
{
    public function test_export_empty_metrics_returns_true() : void
    {
        $exporter = new MemoryMetricExporter();

        self::assertTrue($exporter->export([]));
        self::assertSame([], $exporter->metrics());
    }

    public function test_export_multiple_metrics() : void
    {
        $exporter = new MemoryMetricExporter();
        $metric1 = $this->createMetric('metric-1', 10);
        $metric2 = $this->createMetric('metric-2', 20);
        $metric3 = $this->createMetric('metric-3', 30);

        $exporter->export([$metric1, $metric2]);
        $exporter->export([$metric3]);

        self::assertCount(3, $exporter->metrics());
        self::assertSame($metric1, $exporter->metrics()[0]);
        self::assertSame($metric2, $exporter->metrics()[1]);
        self::assertSame($metric3, $exporter->metrics()[2]);
    }

    public function test_export_single_metric() : void
    {
        $exporter = new MemoryMetricExporter();
        $metric = $this->createMetric('test-metric', 42);

        $result = $exporter->export([$metric]);

        self::assertTrue($result);
        self::assertCount(1, $exporter->metrics());
        self::assertSame($metric, $exporter->metrics()[0]);
    }

    public function test_implements_metric_exporter() : void
    {
        self::assertInstanceOf(MetricExporter::class, new MemoryMetricExporter());
    }

    public function test_reset_clears_all_metrics() : void
    {
        $exporter = new MemoryMetricExporter();
        $exporter->export([$this->createMetric('metric-1', 10), $this->createMetric('metric-2', 20)]);

        self::assertCount(2, $exporter->metrics());

        $exporter->reset();

        self::assertSame([], $exporter->metrics());
    }

    public function test_transports_returns_void_transport() : void
    {
        $exporter = new MemoryMetricExporter();
        $transports = $exporter->transports();

        self::assertCount(1, $transports);
        self::assertInstanceOf(VoidTransport::class, $transports[0]);
    }

    private function createMetric(string $name, int|float $value, MetricType $type = MetricType::COUNTER) : Metric
    {
        return new Metric(
            name: $name,
            type: $type,
            value: $value,
            attributes: Attributes::empty(),
            timestamp: new \DateTimeImmutable(),
            resource: ResourceMother::default(),
            scope: InstrumentationScopeMother::default(),
        );
    }
}
