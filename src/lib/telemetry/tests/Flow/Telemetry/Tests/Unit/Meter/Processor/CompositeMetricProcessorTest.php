<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter\Processor;

use Flow\Telemetry\Attributes;
use Flow\Telemetry\Meter\{Metric, MetricProcessor, MetricType};
use Flow\Telemetry\Meter\Processor\CompositeMetricProcessor;
use Flow\Telemetry\Tests\Mother\{InstrumentationScopeMother, ResourceMother};
use PHPUnit\Framework\TestCase;

final class CompositeMetricProcessorTest extends TestCase
{
    public function test_flush_returns_false_when_any_fails() : void
    {
        $processor1 = $this->createMock(MetricProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(MetricProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(false);

        $composite = new CompositeMetricProcessor([$processor1, $processor2]);

        self::assertFalse($composite->flush());
    }

    public function test_flush_returns_true_when_all_succeed() : void
    {
        $processor1 = $this->createMock(MetricProcessor::class);
        $processor1->expects(self::once())->method('flush')->willReturn(true);

        $processor2 = $this->createMock(MetricProcessor::class);
        $processor2->expects(self::once())->method('flush')->willReturn(true);

        $composite = new CompositeMetricProcessor([$processor1, $processor2]);

        self::assertTrue($composite->flush());
    }

    public function test_forwards_process_to_all_processors() : void
    {
        $processor1 = $this->createMock(MetricProcessor::class);
        $processor1->expects(self::once())->method('process');

        $processor2 = $this->createMock(MetricProcessor::class);
        $processor2->expects(self::once())->method('process');

        $composite = new CompositeMetricProcessor([$processor1, $processor2]);
        $composite->process($this->createMetric());
    }

    public function test_works_with_empty_processors_array() : void
    {
        $composite = new CompositeMetricProcessor([]);

        $composite->process($this->createMetric());

        self::assertTrue($composite->flush());
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
