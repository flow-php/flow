<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Processor;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Meter\Processor\PassThroughMetricProcessor;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Signal\SignalType;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class PassThroughMetricProcessorTest extends TestCase
{
    public function test_exports_each_metric_individually(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter
            ->expects(self::exactly(3))
            ->method('export')
            ->with(static::callback(
                static fn(mixed $signal) => (
                    $signal instanceof Signals
                    && $signal->type === SignalType::METRICS
                    && $signal->count() === 1
                ),
            ))
            ->willReturn(true);

        $processor = new PassThroughMetricProcessor($exporter);
        $processor->process($this->createMetric());
        $processor->process($this->createMetric());
        $processor->process($this->createMetric());
    }

    public function test_exports_metric_immediately_on_process(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter
            ->expects(self::once())
            ->method('export')
            ->with(static::callback(
                static fn(mixed $signal) => (
                    $signal instanceof Signals
                    && $signal->type === SignalType::METRICS
                    && $signal->count() === 1
                ),
            ))
            ->willReturn(true);

        $processor = new PassThroughMetricProcessor($exporter);
        $processor->process($this->createMetric());
    }

    public function test_flush_returns_true(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $processor = new PassThroughMetricProcessor($exporter);

        $result = $processor->flush();

        static::assertTrue($result);
    }

    public function test_process_routes_exporter_throwable_to_error_handler(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->method('export')->willThrowException(new \RuntimeException('exporter exploded'));
        $spy = new ErrorHandlerSpy();

        $processor = new PassThroughMetricProcessor($exporter, $spy);
        $processor->process($this->createMetric());

        static::assertSame(1, $spy->count());
        static::assertSame('exporter exploded', $spy->last()?->getMessage());
    }

    private function createMetric(): Metric
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
