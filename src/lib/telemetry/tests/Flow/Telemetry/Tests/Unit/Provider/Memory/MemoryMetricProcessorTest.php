<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Provider\Memory;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricProcessor;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class MemoryMetricProcessorTest extends TestCase
{
    public function test_count_metrics_returns_correct_count(): void
    {
        $processor = new MemoryMetricProcessor(new MemoryExporter());

        static::assertSame(0, $processor->countMetrics());

        $processor->process($this->createMetric('metric-1', 10));
        static::assertSame(1, $processor->countMetrics());

        $processor->process($this->createMetric('metric-2', 20));
        static::assertSame(2, $processor->countMetrics());
    }

    public function test_flush_exports_metrics(): void
    {
        $exporter = new MemoryExporter();
        $processor = new MemoryMetricProcessor($exporter);
        $metric = $this->createMetric('test-metric', 42);

        $processor->process($metric);
        $result = $processor->flush();

        static::assertTrue($result);
        static::assertCount(1, $exporter->metrics());
        static::assertSame($metric, $exporter->metrics()[0]);
    }

    public function test_flush_returns_true_when_no_metrics(): void
    {
        $processor = new MemoryMetricProcessor(new MemoryExporter());

        static::assertTrue($processor->flush());
    }

    public function test_flush_routes_exporter_throwable_to_error_handler(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->method('export')->willThrowException(new \RuntimeException('exporter exploded'));
        $spy = new ErrorHandlerSpy();

        $processor = new MemoryMetricProcessor($exporter, $spy);
        $processor->process($this->createMetric('metric-1', 10));

        static::assertFalse($processor->flush());
        static::assertSame(1, $spy->count());
        static::assertSame('exporter exploded', $spy->last()?->getMessage());
    }

    public function test_implements_metric_processor(): void
    {
        static::assertInstanceOf(MetricProcessor::class, new MemoryMetricProcessor(new MemoryExporter()));
    }

    public function test_metrics_of_type_filters_correctly(): void
    {
        $processor = new MemoryMetricProcessor(new MemoryExporter());
        $counter = $this->createMetric('requests', 100, MetricType::COUNTER);
        $gauge = $this->createMetric('memory', 1024, MetricType::GAUGE);
        $histogram = $this->createMetric('latency', 50.5, MetricType::HISTOGRAM);

        $processor->process($counter);
        $processor->process($gauge);
        $processor->process($histogram);

        $counters = $processor->metricsOfType(MetricType::COUNTER);
        static::assertCount(1, $counters);
        static::assertSame($counter, $counters[0]);

        $gauges = $processor->metricsOfType(MetricType::GAUGE);
        static::assertCount(1, $gauges);
        static::assertSame($gauge, $gauges[0]);

        $histograms = $processor->metricsOfType(MetricType::HISTOGRAM);
        static::assertCount(1, $histograms);
        static::assertSame($histogram, $histograms[0]);
    }

    public function test_metrics_returns_all_processed_metrics(): void
    {
        $processor = new MemoryMetricProcessor(new MemoryExporter());
        $metric1 = $this->createMetric('metric-1', 10);
        $metric2 = $this->createMetric('metric-2', 20);

        $processor->process($metric1);
        $processor->process($metric2);

        static::assertCount(2, $processor->metrics());
        static::assertSame($metric1, $processor->metrics()[0]);
        static::assertSame($metric2, $processor->metrics()[1]);
    }

    public function test_metrics_with_name_filters_correctly(): void
    {
        $processor = new MemoryMetricProcessor(new MemoryExporter());
        $requests1 = $this->createMetric('http.requests', 100);
        $requests2 = $this->createMetric('http.requests', 150);
        $memory = $this->createMetric('memory.usage', 1024);

        $processor->process($requests1);
        $processor->process($requests2);
        $processor->process($memory);

        $httpRequests = $processor->metricsWithName('http.requests');
        static::assertCount(2, $httpRequests);
        static::assertSame($requests1, $httpRequests[0]);
        static::assertSame($requests2, $httpRequests[1]);

        $memoryUsage = $processor->metricsWithName('memory.usage');
        static::assertCount(1, $memoryUsage);
        static::assertSame($memory, $memoryUsage[0]);
    }

    public function test_process_stores_metric(): void
    {
        $processor = new MemoryMetricProcessor(new MemoryExporter());
        $metric = $this->createMetric('test-metric', 42);

        $processor->process($metric);

        static::assertCount(1, $processor->metrics());
        static::assertSame($metric, $processor->metrics()[0]);
    }

    public function test_reset_clears_all_metrics(): void
    {
        $processor = new MemoryMetricProcessor(new MemoryExporter());

        $processor->process($this->createMetric('metric-1', 10));
        $processor->process($this->createMetric('metric-2', 20));

        static::assertSame(2, $processor->countMetrics());

        $processor->reset();

        static::assertSame(0, $processor->countMetrics());
        static::assertSame([], $processor->metrics());
    }

    private function createMetric(string $name, int|float $value, MetricType $type = MetricType::COUNTER): Metric
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
