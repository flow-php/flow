<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter;

use Flow\Telemetry\Meter\{Meter, MeterProvider};
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Tests\Mother\{ClockMother, ResourceMother};
use PHPUnit\Framework\TestCase;

final class MemoryMeterProviderTest extends TestCase
{
    public function test_creates_new_meter_each_time() : void
    {
        $provider = new MeterProvider($this->createProcessor(), ClockMother::frozen());

        $meter1 = $provider->meter(ResourceMother::default(), 'test', '1.0.0');
        $meter2 = $provider->meter(ResourceMother::default(), 'test', '1.0.0');

        self::assertNotSame($meter1, $meter2);
        self::assertSame($meter1->name(), $meter2->name());
        self::assertSame($meter1->version(), $meter2->version());
    }

    public function test_meter_collects_metrics_on_flush() : void
    {
        $processor = $this->createProcessor();
        $provider = new MeterProvider($processor, ClockMother::frozen());

        $meter = $provider->meter(ResourceMother::default(), 'test');
        $counter = $meter->createCounter('requests.total');
        $counter->add(5);

        $meter->flush();

        self::assertCount(1, $processor->metrics());
        self::assertSame('requests.total', $processor->metrics()[0]->name);
        self::assertSame(5, $processor->metrics()[0]->value);
    }

    public function test_meter_returns_meter_instance() : void
    {
        self::assertInstanceOf(
            Meter::class,
            (new MeterProvider($this->createProcessor(), ClockMother::frozen()))->meter(ResourceMother::default(), 'test-lib', '1.0.0')
        );
    }

    public function test_metrics_returns_empty_before_flush() : void
    {
        $processor = $this->createProcessor();
        $provider = new MeterProvider($processor, ClockMother::frozen());

        $meter = $provider->meter(ResourceMother::default(), 'test');
        $counter = $meter->createCounter('requests.total');
        $counter->add(5);

        self::assertSame([], $processor->metrics());
    }

    public function test_processor_flush_returns_true() : void
    {
        $processor = $this->createProcessor();

        self::assertTrue($processor->flush());
    }

    public function test_uses_default_version_when_not_provided() : void
    {
        $provider = new MeterProvider($this->createProcessor(), ClockMother::frozen());

        $meter = $provider->meter(ResourceMother::default(), 'test');
        self::assertSame('unknown', $meter->version());
    }

    private function createProcessor() : MemoryMetricProcessor
    {
        return new MemoryMetricProcessor(new VoidExporter());
    }
}
