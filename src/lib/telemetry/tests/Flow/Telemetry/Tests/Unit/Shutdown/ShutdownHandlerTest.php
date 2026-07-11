<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Shutdown;

use ArrayObject;
use Flow\Telemetry\Shutdown\ShutdownHandler;
use Flow\Telemetry\Tests\Mother\ShutdownOrderSpanProcessor;
use Flow\Telemetry\Tests\Mother\SpanProcessorSpy;
use Flow\Telemetry\Tests\Mother\TelemetryMother;
use PHPUnit\Framework\TestCase;

use function gc_collect_cycles;

final class ShutdownHandlerTest extends TestCase
{
    protected function setUp(): void
    {
        ShutdownHandler::invoke();
    }

    public function test_invoke_shuts_down_registered_telemetry(): void
    {
        $processor = new SpanProcessorSpy();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        ShutdownHandler::register($telemetry);
        ShutdownHandler::invoke();

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

        ShutdownHandler::register($collectedTelemetry);
        ShutdownHandler::register($keptTelemetry);

        unset($collectedTelemetry);
        gc_collect_cycles();

        ShutdownHandler::invoke();

        static::assertSame(0, $collectedProcessor->shutdownCount());
        static::assertSame(1, $keptProcessor->shutdownCount());
    }

    public function test_invoke_runs_registrations_once(): void
    {
        $processor = new SpanProcessorSpy();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        ShutdownHandler::register($telemetry);
        ShutdownHandler::invoke();
        ShutdownHandler::invoke();

        static::assertSame(1, $processor->shutdownCount());
    }

    public function test_register_after_invoke_arms_the_handler_again(): void
    {
        $firstProcessor = new SpanProcessorSpy();
        $firstTelemetry = TelemetryMother::withSpanProcessor($firstProcessor);
        $firstTelemetry->tracer('test');

        ShutdownHandler::register($firstTelemetry);
        ShutdownHandler::invoke();

        $secondProcessor = new SpanProcessorSpy();
        $secondTelemetry = TelemetryMother::withSpanProcessor($secondProcessor);
        $secondTelemetry->tracer('test');

        ShutdownHandler::register($secondTelemetry);
        ShutdownHandler::invoke();

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

        ShutdownHandler::register($firstTelemetry);
        ShutdownHandler::register($secondTelemetry);
        ShutdownHandler::invoke();

        static::assertSame(['second', 'first'], $shutdownLog->getArrayCopy());
    }
}
