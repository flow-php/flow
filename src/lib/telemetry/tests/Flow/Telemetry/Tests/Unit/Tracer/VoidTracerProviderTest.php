<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Tracer;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Provider\Void\VoidSpanProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Tests\Mother\ClockMother;
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\Tracer;
use Flow\Telemetry\Tracer\TracerProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Telemetry\DSL\context;

final class VoidTracerProviderTest extends TestCase
{
    private Resource $resource;

    protected function setUp(): void
    {
        $this->resource = ResourceMother::default();
    }

    public function test_context_storage_applies_to_new_tracers(): void
    {
        $ctx = context();
        $storage = new MemoryContextStorage($ctx);

        $provider = new TracerProvider(new VoidSpanProcessor(), ClockMother::frozen(), $storage);

        static::assertSame(
            $ctx->traceId->toHex(),
            $provider->tracer($this->resource, 'test')->context()->traceId->toHex(),
        );
    }

    public function test_processor_flush_returns_true(): void
    {
        $processor = new VoidSpanProcessor();

        static::assertTrue($processor->flush());
    }

    public function test_tracer_creates_functional_tracer(): void
    {
        $tracer = (new TracerProvider(
            new VoidSpanProcessor(),
            ClockMother::frozen(),
            new MemoryContextStorage(),
        ))->tracer($this->resource, 'my-library', '1.0.0');
        $span = $tracer->span('test-operation');

        static::assertSame('test-operation', $span->name());

        $tracer->complete($span);

        static::assertTrue($span->isEnded());
    }

    public function test_tracer_returns_tracer(): void
    {
        $tracer = (new TracerProvider(
            new VoidSpanProcessor(),
            ClockMother::frozen(),
            new MemoryContextStorage(),
        ))->tracer($this->resource, 'my-library', '1.0.0');

        static::assertInstanceOf(Tracer::class, $tracer);
        static::assertSame('my-library', $tracer->name());
        static::assertSame('1.0.0', $tracer->version());
    }

    public function test_tracer_uses_unknown_as_default_version(): void
    {
        static::assertSame(
            'unknown',
            (new TracerProvider(new VoidSpanProcessor(), ClockMother::frozen(), new MemoryContextStorage()))
                ->tracer($this->resource, 'my-library')
                ->version(),
        );
    }
}
