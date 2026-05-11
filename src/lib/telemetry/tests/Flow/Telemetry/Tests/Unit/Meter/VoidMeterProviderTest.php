<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Meter;

use Flow\Telemetry\Meter\Meter;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use PHPUnit\Framework\TestCase;

final class VoidMeterProviderTest extends TestCase
{
    private Resource $resource;

    protected function setUp(): void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_meter_creates_functional_meter(): void
    {
        $provider = $this->createProvider();
        $meter = $provider->meter($this->resource, 'my-library', '1.0.0');

        $meter->createCounter('requests.total')->add(5);
        $meter->createGauge('cpu.usage')->record(75.5);
        $meter->createHistogram('request.duration')->record(125);
        $meter->createUpDownCounter('queue.size')->add(-3);

        static::assertSame('my-library', $meter->name());
        static::assertSame('1.0.0', $meter->version());
    }

    public function test_meter_returns_meter(): void
    {
        $meter = $this->createProvider()->meter($this->resource, 'my-library', '1.0.0');

        static::assertInstanceOf(Meter::class, $meter);
        static::assertSame('my-library', $meter->name());
        static::assertSame('1.0.0', $meter->version());
    }

    public function test_meter_uses_unknown_as_default_version(): void
    {
        static::assertSame('unknown', $this->createProvider()->meter($this->resource, 'my-library')->version());
    }

    public function test_processor_flush_returns_true(): void
    {
        $processor = new VoidMetricProcessor();

        static::assertTrue($processor->flush());
    }

    private function createProvider(): MeterProvider
    {
        return new MeterProvider(new VoidMetricProcessor(), ClockMother::frozen());
    }
}
