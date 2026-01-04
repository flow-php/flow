<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Memory;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Meter\{Metric, MetricProcessor, MetricType};
use Flow\Telemetry\Provider\Memory\{MemoryMetricExporter, MemoryMetricProcessor};
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, ResourceMother};
use PHPUnit\Framework\TestCase;

final class MemoryMetricProcessorTest extends TestCase
{
    public function test_count_metrics_returns_correct_count() : void
    {
        $processor = new MemoryMetricProcessor(new MemoryMetricExporter());

        self::assertSame(0, $processor->countMetrics());

        $processor->process($this->createMetric('metric-1', 10));
        self::assertSame(1, $processor->countMetrics());

        $processor->process($this->createMetric('metric-2', 20));
        self::assertSame(2, $processor->countMetrics());
    }

    public function test_exporter_returns_configured_exporter() : void
    {
        $exporter = new MemoryMetricExporter();
        $processor = new MemoryMetricProcessor($exporter);

        self::assertSame($exporter, $processor->exporter());
    }

    public function test_flush_exports_metrics() : void
    {
        $exporter = new MemoryMetricExporter();
        $processor = new MemoryMetricProcessor($exporter);
        $metric = $this->createMetric('test-metric', 42);

        $processor->process($metric);
        $result = $processor->flush();

        self::assertTrue($result);
        self::assertCount(1, $exporter->metrics());
        self::assertSame($metric, $exporter->metrics()[0]);
    }

    public function test_flush_returns_true_when_no_metrics() : void
    {
        $processor = new MemoryMetricProcessor(new MemoryMetricExporter());

        self::assertTrue($processor->flush());
    }

    public function test_implements_metric_processor() : void
    {
        self::assertInstanceOf(MetricProcessor::class, new MemoryMetricProcessor(new MemoryMetricExporter()));
    }

    public function test_metrics_of_type_filters_correctly() : void
    {
        $processor = new MemoryMetricProcessor(new MemoryMetricExporter());
        $counter = $this->createMetric('requests', 100, MetricType::COUNTER);
        $gauge = $this->createMetric('memory', 1024, MetricType::GAUGE);
        $histogram = $this->createMetric('latency', 50.5, MetricType::HISTOGRAM);

        $processor->process($counter);
        $processor->process($gauge);
        $processor->process($histogram);

        $counters = $processor->metricsOfType(MetricType::COUNTER);
        self::assertCount(1, $counters);
        self::assertSame($counter, $counters[0]);

        $gauges = $processor->metricsOfType(MetricType::GAUGE);
        self::assertCount(1, $gauges);
        self::assertSame($gauge, $gauges[0]);

        $histograms = $processor->metricsOfType(MetricType::HISTOGRAM);
        self::assertCount(1, $histograms);
        self::assertSame($histogram, $histograms[0]);
    }

    public function test_metrics_returns_all_processed_metrics() : void
    {
        $processor = new MemoryMetricProcessor(new MemoryMetricExporter());
        $metric1 = $this->createMetric('metric-1', 10);
        $metric2 = $this->createMetric('metric-2', 20);

        $processor->process($metric1);
        $processor->process($metric2);

        self::assertCount(2, $processor->metrics());
        self::assertSame($metric1, $processor->metrics()[0]);
        self::assertSame($metric2, $processor->metrics()[1]);
    }

    public function test_metrics_with_name_filters_correctly() : void
    {
        $processor = new MemoryMetricProcessor(new MemoryMetricExporter());
        $requests1 = $this->createMetric('http.requests', 100);
        $requests2 = $this->createMetric('http.requests', 150);
        $memory = $this->createMetric('memory.usage', 1024);

        $processor->process($requests1);
        $processor->process($requests2);
        $processor->process($memory);

        $httpRequests = $processor->metricsWithName('http.requests');
        self::assertCount(2, $httpRequests);
        self::assertSame($requests1, $httpRequests[0]);
        self::assertSame($requests2, $httpRequests[1]);

        $memoryUsage = $processor->metricsWithName('memory.usage');
        self::assertCount(1, $memoryUsage);
        self::assertSame($memory, $memoryUsage[0]);
    }

    public function test_process_stores_metric() : void
    {
        $processor = new MemoryMetricProcessor(new MemoryMetricExporter());
        $metric = $this->createMetric('test-metric', 42);

        $processor->process($metric);

        self::assertCount(1, $processor->metrics());
        self::assertSame($metric, $processor->metrics()[0]);
    }

    public function test_reset_clears_all_metrics() : void
    {
        $processor = new MemoryMetricProcessor(new MemoryMetricExporter());

        $processor->process($this->createMetric('metric-1', 10));
        $processor->process($this->createMetric('metric-2', 20));

        self::assertSame(2, $processor->countMetrics());

        $processor->reset();

        self::assertSame(0, $processor->countMetrics());
        self::assertSame([], $processor->metrics());
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
