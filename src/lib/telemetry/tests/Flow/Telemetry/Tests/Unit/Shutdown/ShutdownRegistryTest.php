<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Shutdown;

use ArrayObject;
use Flow\Telemetry\Shutdown\ShutdownRegistry;
use Flow\Telemetry\Tests\Mother\ShutdownOrderSpanProcessor;
use Flow\Telemetry\Tests\Mother\SpanProcessorSpy;
use Flow\Telemetry\Tests\Mother\TelemetryMother;
use PHPUnit\Framework\TestCase;

use function gc_collect_cycles;

final class ShutdownRegistryTest extends TestCase
{
    protected function setUp(): void
    {
        ShutdownRegistry::invoke();
    }

    public function test_invoke_shuts_down_registered_telemetry(): void
    {
        $processor = new SpanProcessorSpy();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        ShutdownRegistry::register($telemetry);
        ShutdownRegistry::invoke();

        static::assertSame(1, $processor->shutdownCount());
    }

    public function test_invoke_skips_garbage_collected_instances(): void
    {
        $collectedProcessor = new SpanProcessorSpy();
        $collectedTelemetry = TelemetryMother::withSpanProcessor($collectedProcessor);
        $collectedTelemetry->tracer('test');

        $keptProcessor = new SpanProcessorSpy();
        $keptTelemetry = TelemetryMother::withSpanProcessor($keptProcessor);
        $keptTelemetry->tracer('test');

        ShutdownRegistry::register($collectedTelemetry);
        ShutdownRegistry::register($keptTelemetry);

        unset($collectedTelemetry);
        gc_collect_cycles();

        ShutdownRegistry::invoke();

        static::assertSame(0, $collectedProcessor->shutdownCount());
        static::assertSame(1, $keptProcessor->shutdownCount());
    }

    public function test_invoke_runs_registrations_once(): void
    {
        $processor = new SpanProcessorSpy();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        ShutdownRegistry::register($telemetry);
        ShutdownRegistry::invoke();
        ShutdownRegistry::invoke();

        static::assertSame(1, $processor->shutdownCount());
    }

    public function test_register_after_invoke_arms_the_handler_again(): void
    {
        $firstProcessor = new SpanProcessorSpy();
        $firstTelemetry = TelemetryMother::withSpanProcessor($firstProcessor);
        $firstTelemetry->tracer('test');

        ShutdownRegistry::register($firstTelemetry);
        ShutdownRegistry::invoke();

        $secondProcessor = new SpanProcessorSpy();
        $secondTelemetry = TelemetryMother::withSpanProcessor($secondProcessor);
        $secondTelemetry->tracer('test');

        ShutdownRegistry::register($secondTelemetry);
        ShutdownRegistry::invoke();

        static::assertSame(1, $firstProcessor->shutdownCount());
        static::assertSame(1, $secondProcessor->shutdownCount());
    }

    public function test_invoke_shuts_down_most_recently_registered_first(): void
    {
        /** @var ArrayObject<int, string> $shutdownLog */
        $shutdownLog = new ArrayObject();

        $firstTelemetry = TelemetryMother::withSpanProcessor(new ShutdownOrderSpanProcessor('first', $shutdownLog));
        $firstTelemetry->tracer('test');

        $secondTelemetry = TelemetryMother::withSpanProcessor(new ShutdownOrderSpanProcessor('second', $shutdownLog));
        $secondTelemetry->tracer('test');

        ShutdownRegistry::register($firstTelemetry);
        ShutdownRegistry::register($secondTelemetry);
        ShutdownRegistry::invoke();

        static::assertSame(['second', 'first'], $shutdownLog->getArrayCopy());
    }
}
