<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use function Flow\Telemetry\DSL\context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Provider\Void\VoidSpanProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\{ClockMother, ResourceMother};
use Flow\Telemetry\Tracer\{Tracer, TracerProvider};
use PHPUnit\Framework\TestCase;

final class VoidTracerProviderTest extends TestCase
{
    private Resource $resource;

    protected function setUp() : void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_context_storage_applies_to_new_tracers() : void
    {
        $ctx = context();
        $storage = new MemoryContextStorage($ctx);

        $provider = new TracerProvider(new VoidSpanProcessor(), ClockMother::frozen(), $storage);

        self::assertSame($ctx->traceId->toHex(), $provider->tracer($this->resource, 'test')->context()->traceId->toHex());
    }

    public function test_processor_flush_returns_true() : void
    {
        $processor = new VoidSpanProcessor();

        self::assertTrue($processor->flush());
    }

    public function test_tracer_creates_functional_tracer() : void
    {
        $tracer = (new TracerProvider(new VoidSpanProcessor(), ClockMother::frozen(), new MemoryContextStorage()))->tracer($this->resource, 'my-library', '1.0.0');
        $span = $tracer->span('test-operation');

        self::assertSame('test-operation', $span->name());

        $tracer->complete($span);

        self::assertTrue($span->isEnded());
    }

    public function test_tracer_returns_tracer() : void
    {
        $tracer = (new TracerProvider(new VoidSpanProcessor(), ClockMother::frozen(), new MemoryContextStorage()))->tracer($this->resource, 'my-library', '1.0.0');

        self::assertInstanceOf(Tracer::class, $tracer);
        self::assertSame('my-library', $tracer->name());
        self::assertSame('1.0.0', $tracer->version());
    }

    public function test_tracer_uses_unknown_as_default_version() : void
    {
        self::assertSame(
            'unknown',
            (new TracerProvider(new VoidSpanProcessor(), ClockMother::frozen(), new MemoryContextStorage()))->tracer($this->resource, 'my-library')->version()
        );
    }
}
