<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Processor;

use DateTimeImmutable;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricProcessor;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Meter\Processor\CompositeMetricProcessor;
use Flow\Telemetry\Tests\Mother\ErrorHandlerSpy;
use Flow\Telemetry\Tests\Mother\InstrumentationScopeMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class CompositeMetricProcessorTest extends TestCase
{
    public function test_flush_continues_after_child_throws_and_routes_to_error_handler(): void
    {
        $throwing = $this->createStub(MetricProcessor::class);
        $throwing->method('flush')->willThrowException(new RuntimeException('flush blew up'));

        $sibling = $this->createMock(MetricProcessor::class);
        $sibling->expects(self::once())->method('flush')->willReturn(true);

        $spy = new ErrorHandlerSpy();
        $composite = new CompositeMetricProcessor([$throwing, $sibling], $spy);

        static::assertFalse($composite->flush());
        static::assertSame(1, $spy->count());
    }

    public function test_flush_returns_false_when_any_fails(): void
    {
        $processor1 = $this->createMock(MetricProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(MetricProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(false);

        $composite = new CompositeMetricProcessor([$processor1, $processor2]);

        static::assertFalse($composite->flush());
    }

    public function test_flush_returns_true_when_all_succeed(): void
    {
        $processor1 = $this->createMock(MetricProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(MetricProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(true);

        $composite = new CompositeMetricProcessor([$processor1, $processor2]);

        static::assertTrue($composite->flush());
    }

    public function test_forwards_process_to_all_processors(): void
    {
        $processor1 = $this->createMock(MetricProcessor::class);
        $processor1->expects(self::once())->method('process');

        $processor2 = $this->createMock(MetricProcessor::class);
        $processor2->expects(self::once())->method('process');

        $composite = new CompositeMetricProcessor([$processor1, $processor2]);
        $composite->process($this->createMetric());
    }

    public function test_process_continues_after_child_throws_and_routes_to_error_handler(): void
    {
        $throwing = $this->createStub(MetricProcessor::class);
        $throwing->method('process')->willThrowException(new RuntimeException('child blew up'));

        $sibling = $this->createMock(MetricProcessor::class);
        $sibling->expects(self::once())->method('process');

        $spy = new ErrorHandlerSpy();
        $composite = new CompositeMetricProcessor([$throwing, $sibling], $spy);
        $composite->process($this->createMetric());

        static::assertSame(1, $spy->count());
    }

    public function test_works_with_empty_processors_array(): void
    {
        $composite = new CompositeMetricProcessor([]);

        $composite->process($this->createMetric());

        static::assertTrue($composite->flush());
    }

    private function createMetric(): Metric
    {
        return new Metric(
            'test.counter',
            MetricType::COUNTER,
            1,
            Attributes::create(['key' => 'value']),
            new DateTimeImmutable(),
            ResourceMother::default(),
            InstrumentationScopeMother::default(),
        );
    }
}
