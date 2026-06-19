<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Context\SpanId;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Provider\Void\VoidExporter;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\SpanContext;
use Flow\Telemetry\Tracer\Tracer;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\TestCase;

final class MemoryTracerProviderTest extends TestCase
{
    private Resource $resource;

    protected function setUp(): void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_context_storage_applies_to_new_tracers(): void
    {
        $span = SpanContext::create(TraceId::generate(), SpanId::generate());
        $storage = new MemoryContextStorage(Context::root()->withActiveSpan($span));

        $provider = new TracerProvider($this->createProcessor(), ClockMother::frozen(), $storage);

        static::assertSame(
            $span->spanId->toHex(),
            $provider->tracer($this->resource, 'test')->context()->activeSpanId()?->toHex(),
        );
    }

    public function test_creates_new_tracer_each_time(): void
    {
        $provider = new TracerProvider($this->createProcessor(), ClockMother::frozen(), new MemoryContextStorage());

        $tracer1 = $provider->tracer($this->resource, 'test', '1.0.0');
        $tracer2 = $provider->tracer($this->resource, 'test', '1.0.0');

        static::assertNotSame($tracer1, $tracer2);
        static::assertSame($tracer1->name(), $tracer2->name());
        static::assertSame($tracer1->version(), $tracer2->version());
    }

    public function test_processor_flush_returns_true(): void
    {
        $processor = $this->createProcessor();

        static::assertTrue($processor->flush());
    }

    public function test_processor_records_spans_from_tracers(): void
    {
        $processor = $this->createProcessor();
        $provider = new TracerProvider($processor, ClockMother::frozen(), new MemoryContextStorage());

        $tracer = $provider->tracer($this->resource, 'test');
        $span = $tracer->span('operation');
        $tracer->complete($span);

        static::assertCount(1, $processor->endedSpans());
        static::assertSame('operation', $processor->endedSpans()[0]->name());
    }

    public function test_tracer_returns_tracer_instance(): void
    {
        static::assertInstanceOf(Tracer::class, (new TracerProvider(
            $this->createProcessor(),
            ClockMother::frozen(),
            new MemoryContextStorage(),
        ))->tracer($this->resource, 'test-lib', '1.0.0'));
    }

    public function test_uses_default_version_when_not_provided(): void
    {
        $provider = new TracerProvider($this->createProcessor(), ClockMother::frozen(), new MemoryContextStorage());

        $tracer = $provider->tracer($this->resource, 'test');
        static::assertSame('unknown', $tracer->version());
    }

    private function createProcessor(): MemorySpanProcessor
    {
        return new MemorySpanProcessor(new VoidExporter());
    }
}
