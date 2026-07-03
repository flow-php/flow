<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\ConsoleFlushSubscriber;
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
    public function test_terminate_flushes_telemetry_without_shutting_it_down(): void
    {
        $processor = new SpySpanProcessor();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        $subscriber = new ConsoleFlushSubscriber($telemetry, []);

        $subscriber->onTerminate(
            new ConsoleTerminateEvent(new Command('test'), new ArrayInput([]), new NullOutput(), 0),
        );

        static::assertSame(1, $processor->flushCount);
        static::assertSame(0, $processor->shutdownCount);
    }

    public function test_every_terminate_flushes_again(): void
    {
        $processor = new SpySpanProcessor();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        $subscriber = new ConsoleFlushSubscriber($telemetry, []);

        $event = new ConsoleTerminateEvent(new Command('test'), new ArrayInput([]), new NullOutput(), 0);
        $subscriber->onTerminate($event);
        $subscriber->onTerminate($event);

        static::assertSame(2, $processor->flushCount);
        static::assertSame(0, $processor->shutdownCount);
    }
}
