<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Processor;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Meter\{Metric, MetricExporter, MetricType};
use Flow\Telemetry\Meter\Processor\PassThroughMetricProcessor;
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, ResourceMother};
use PHPUnit\Framework\TestCase;

final class PassThroughMetricProcessorTest extends TestCase
{
    public function test_exports_each_metric_individually() : void
    {
        $exporter = $this->createMock(MetricExporter::class);
        $exporter->expects(self::exactly(3))
            ->method('export')
            ->with(self::callback(fn (array $metrics) => \count($metrics) === 1))
            ->willReturn(true);

        $processor = new PassThroughMetricProcessor($exporter);
        $processor->process($this->createMetric());
        $processor->process($this->createMetric());
        $processor->process($this->createMetric());
    }

    public function test_exports_metric_immediately_on_process() : void
    {
        $exporter = $this->createMock(MetricExporter::class);
        $exporter->expects(self::once())
            ->method('export')
            ->with(self::callback(fn (array $metrics) => \count($metrics) === 1))
            ->willReturn(true);

        $processor = new PassThroughMetricProcessor($exporter);
        $processor->process($this->createMetric());
    }

    public function test_flush_returns_true() : void
    {
        $exporter = $this->createMock(MetricExporter::class);
        $processor = new PassThroughMetricProcessor($exporter);

        $result = $processor->flush();

        self::assertTrue($result);
    }

    private function createMetric() : Metric
    {
        return new Metric(
            'test.counter',
            MetricType::COUNTER,
            1,
            Attributes::create(['key' => 'value']),
            new \DateTimeImmutable(),
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
        );
    }
}
