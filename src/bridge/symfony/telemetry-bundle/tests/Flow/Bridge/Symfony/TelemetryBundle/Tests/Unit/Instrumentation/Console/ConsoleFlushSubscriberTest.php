<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\ConsoleFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Runtime\RuntimeModeResolver;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Runtime\StubWorkerModeDetector;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Telemetry\SpySpanProcessor;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;

#[CoversClass(ConsoleFlushSubscriber::class)]
final class ConsoleFlushSubscriberTest extends TestCase
{
    public function test_classic_mode_shuts_telemetry_down_on_terminate(): void
    {
        $processor = new SpySpanProcessor();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        $subscriber = new ConsoleFlushSubscriber(
            $telemetry,
            new RuntimeModeResolver('classic', new StubWorkerModeDetector(true)),
            [],
        );

        $subscriber->onTerminate(
            new ConsoleTerminateEvent(new Command('test'), new ArrayInput([]), new NullOutput(), 0),
        );

        static::assertSame(1, $processor->shutdownCount);
    }

    public function test_worker_mode_flushes_telemetry_without_shutting_down_on_terminate(): void
    {
        $processor = new SpySpanProcessor();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        $subscriber = new ConsoleFlushSubscriber(
            $telemetry,
            new RuntimeModeResolver('worker', new StubWorkerModeDetector(false)),
            [],
        );

        $subscriber->onTerminate(
            new ConsoleTerminateEvent(new Command('test'), new ArrayInput([]), new NullOutput(), 0),
        );

        static::assertSame(0, $processor->shutdownCount);
        static::assertSame(1, $processor->flushCount);
    }
}
