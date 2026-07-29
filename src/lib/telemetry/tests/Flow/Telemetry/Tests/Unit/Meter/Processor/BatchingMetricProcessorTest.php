<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Processor;

use DateTimeImmutable;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Meter\Processor\BatchingMetricProcessor;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Signal\SignalType;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\ExporterSpy;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\MetricMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class BatchingMetricProcessorTest extends TestCase
{
    public function test_exports_on_batch_size_reached(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter
            ->expects(self::once())
            ->method('export')
            ->with(static::callback(
                static fn(mixed $signal) => (
                    $signal instanceof Signals
                    && $signal->type === SignalType::METRICS
                    && $signal->count() === 2
                ),
            ))
            ->willReturn(true);

        $processor = new BatchingMetricProcessor($exporter, 2);

        $processor->process($this->createMetric());
        $processor->process($this->createMetric());
    }

    public function test_exports_remaining_on_flush(): void
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

        $processor = new BatchingMetricProcessor($exporter, 10);
        $processor->process($this->createMetric());

        $result = $processor->flush();

        static::assertTrue($result);
    }

    public function test_flush_returns_true_when_buffer_empty(): void
    {
        $exporter = $this->createMock(Exporter::class);
        $exporter->expects(self::never())->method('export');

        $processor = new BatchingMetricProcessor($exporter, 10);

        $result = $processor->flush();

        static::assertTrue($result);
    }

    public function test_flush_routes_exporter_throwable_to_error_handler(): void
    {
        $exporter = $this->createStub(Exporter::class);
        $exporter->method('export')->willThrowException(new RuntimeException('exporter exploded'));
        $spy = new ErrorHandlerSpy();

        $processor = new BatchingMetricProcessor($exporter, 10, $spy);
        $processor->process($this->createMetric());

        static::assertFalse($processor->flush());
        static::assertSame(1, $spy->count());
        static::assertSame('exporter exploded', $spy->last()?->getMessage());
    }

    public function test_age_disabled_by_default_does_not_export_until_flush(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingMetricProcessor($exporter, 512, new ErrorHandlerSpy(), null);

        $processor->process(MetricMother::counter('test.metric', 1));
        $processor->process(MetricMother::counter('test.metric', 1));

        static::assertSame(0, $exporter->exportedCount());

        static::assertTrue($processor->flush());
        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_age_set_but_not_due_does_not_export_until_flush(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingMetricProcessor($exporter, 512, new ErrorHandlerSpy(), 3600.0);

        $processor->process(MetricMother::counter('test.metric', 1));
        $processor->process(MetricMother::counter('test.metric', 1));

        static::assertSame(0, $exporter->exportedCount());

        static::assertTrue($processor->flush());
        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_age_flush_when_deadline_exceeded_on_process(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingMetricProcessor($exporter, 512, new ErrorHandlerSpy(), 0.01);

        $processor->process(MetricMother::counter('test.metric', 1));
        usleep(20_000);
        $processor->process(MetricMother::counter('test.metric', 1));

        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_size_trigger_still_flushes_when_age_set_but_not_due(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingMetricProcessor($exporter, 2, new ErrorHandlerSpy(), 3600.0);

        $processor->process(MetricMother::counter('test.metric', 1));
        $processor->process(MetricMother::counter('test.metric', 1));

        static::assertSame(1, $exporter->exportedCount());
        static::assertSame(2, $exporter->exported()[0]->count());
    }

    public function test_age_clock_resets_after_flush(): void
    {
        $exporter = new ExporterSpy();
        $processor = new BatchingMetricProcessor($exporter, 512, new ErrorHandlerSpy(), 0.01);

        $processor->process(MetricMother::counter('test.metric', 1));
        usleep(20_000);
        static::assertTrue($processor->flush());
        static::assertSame(1, $exporter->exportedCount());

        $processor->process(MetricMother::counter('test.metric', 1));

        static::assertSame(1, $exporter->exportedCount());
    }

    private function createMetric(): Metric
    {
        return new Metric(
            'test.metric',
            MetricType::COUNTER,
            1,
            Attributes::empty(),
            new DateTimeImmutable(),
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
        );
    }
}
