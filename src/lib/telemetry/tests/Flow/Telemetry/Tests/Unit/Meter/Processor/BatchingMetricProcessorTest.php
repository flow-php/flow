<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Processor;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Meter\{Metric, MetricType};
use Flow\Telemetry\Meter\Processor\BatchingMetricProcessor;
use Flow\Telemetry\Signal\{SignalType, Signals};
use Flow\Telemetry\Tests\Mother\{ErrorHandlerSpy, InstrumentationScopeMother, ResourceMother};
use PHPUnit\Framework\TestCase;

final class BatchingMetricProcessorTest extends TestCase
{
    public function test_exports_on_batch_size_reached() : void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->expects(self::once())
            ->method('export')
            ->with(self::callback(static fn (mixed $signal) => $signal instanceof Signals && $signal->type === SignalType::METRICS && $signal->count() === 2))
            ->willReturn(true);

        $processor = new BatchingMetricProcessor($exporter, 2);

        $processor->process($this->createMetric());
        $processor->process($this->createMetric());
    }

    public function test_exports_remaining_on_flush() : void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->expects(self::once())
            ->method('export')
            ->with(self::callback(static fn (mixed $signal) => $signal instanceof Signals && $signal->type === SignalType::METRICS && $signal->count() === 1))
            ->willReturn(true);

        $processor = new BatchingMetricProcessor($exporter, 10);
        $processor->process($this->createMetric());

        $result = $processor->flush();

        self::assertTrue($result);
    }

    public function test_flush_returns_true_when_buffer_empty() : void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->expects(self::never())
            ->method('export');

        $processor = new BatchingMetricProcessor($exporter, 10);

        $result = $processor->flush();

        self::assertTrue($result);
    }

    public function test_flush_routes_exporter_throwable_to_error_handler() : void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->method('export')->willThrowException(new \RuntimeException('exporter exploded'));
        $spy = new ErrorHandlerSpy();

        $processor = new BatchingMetricProcessor($exporter, 10, $spy);
        $processor->process($this->createMetric());

        self::assertFalse($processor->flush());
        self::assertSame(1, $spy->count());
        self::assertSame('exporter exploded', $spy->last()?->getMessage());
    }

    private function createMetric() : Metric
    {
        return new Metric(
            'test.metric',
            MetricType::COUNTER,
            1,
            Attributes::empty(),
            new \DateTimeImmutable(),
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
        );
    }
}
