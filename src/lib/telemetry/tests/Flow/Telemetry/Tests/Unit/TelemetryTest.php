<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\Logger;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\MemoryLogProcessor;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\SpanProcessor;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\TestCase;

final class TelemetryTest extends TestCase
{
    private Resource $resource;

    protected function setUp(): void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_flush_returns_false_when_span_processor_fails(): void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        $failingSpanProcessor = $this->createMock(SpanProcessor::class);
        $failingSpanProcessor->method('flush')->willReturn(false);

        $telemetry = new Telemetry(
            $this->resource,
            new TracerProvider($failingSpanProcessor, $clock, $contextStorage),
            new MeterProvider($this->createMetricProcessor(), $clock),
            new LoggerProvider($this->createLogProcessor(), $clock, $contextStorage),
        );

        $telemetry->tracer('test');

        static::assertFalse($telemetry->flush());
    }

    public function test_flush_returns_true_when_all_succeed(): void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();
        $spanProcessor = $this->createSpanProcessor();
        $metricProcessor = $this->createMetricProcessor();
        $logProcessor = $this->createLogProcessor();

        $telemetry = new Telemetry(
            $this->resource,
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        static::assertTrue($telemetry->flush());
    }

    public function test_logger_delegates_to_provider(): void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();
        $spanProcessor = $this->createSpanProcessor();
        $metricProcessor = $this->createMetricProcessor();
        $logProcessor = $this->createLogProcessor();

        $telemetry = new Telemetry(
            $this->resource,
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );
        $logger = $telemetry->logger('test-logger', '1.0.0');

        static::assertInstanceOf(Logger::class, $logger);
    }

    public function test_meter_delegates_to_provider(): void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();
        $spanProcessor = $this->createSpanProcessor();
        $metricProcessor = $this->createMetricProcessor();
        $logProcessor = $this->createLogProcessor();

        $telemetry = new Telemetry(
            $this->resource,
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );
        $meter = $telemetry->meter('test-meter', '1.0.0');

        static::assertSame('test-meter', $meter->name());
        static::assertSame('1.0.0', $meter->version());
    }

    public function test_register_shutdown_function_returns_self(): void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();
        $spanProcessor = $this->createSpanProcessor();
        $metricProcessor = $this->createMetricProcessor();
        $logProcessor = $this->createLogProcessor();

        $telemetry = new Telemetry(
            $this->resource,
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        static::assertSame($telemetry, $telemetry->registerShutdownFunction());
    }

    public function test_shutdown_returns_true_when_all_succeed(): void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();
        $spanProcessor = $this->createSpanProcessor();
        $metricProcessor = $this->createMetricProcessor();
        $logProcessor = $this->createLogProcessor();

        $telemetry = new Telemetry(
            $this->resource,
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );

        static::assertTrue($telemetry->shutdown());
    }

    public function test_tracer_delegates_to_provider(): void
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();
        $spanProcessor = $this->createSpanProcessor();
        $metricProcessor = $this->createMetricProcessor();
        $logProcessor = $this->createLogProcessor();

        $telemetry = new Telemetry(
            $this->resource,
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider($logProcessor, $clock, $contextStorage),
        );
        $tracer = $telemetry->tracer('test-tracer', '1.0.0');

        static::assertSame('test-tracer', $tracer->name());
        static::assertSame('1.0.0', $tracer->version());
    }

    private function createLogProcessor(): MemoryLogProcessor
    {
        return new MemoryLogProcessor(new VoidExporter());
    }

    private function createMetricProcessor(): MemoryMetricProcessor
    {
        return new MemoryMetricProcessor(new VoidExporter());
    }

    private function createSpanProcessor(): MemorySpanProcessor
    {
        return new MemorySpanProcessor(new VoidExporter());
    }
}
