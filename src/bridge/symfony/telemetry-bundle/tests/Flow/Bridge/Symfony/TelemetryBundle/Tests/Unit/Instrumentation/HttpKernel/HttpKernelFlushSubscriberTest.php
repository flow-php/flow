<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\StubHttpKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Telemetry\SpySpanProcessor;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Event\TerminateEvent;

#[CoversClass(HttpKernelFlushSubscriber::class)]
final class HttpKernelFlushSubscriberTest extends TestCase
{
    public function test_terminate_flushes_telemetry_without_shutting_it_down(): void
    {
        $processor = new SpySpanProcessor();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        $subscriber = new HttpKernelFlushSubscriber($telemetry, []);

        $subscriber->onTerminate(new TerminateEvent(new StubHttpKernel(), Request::create('/'), new Response()));

        static::assertSame(1, $processor->flushCount);
        static::assertSame(0, $processor->shutdownCount);
    }

    public function test_every_terminate_flushes_again(): void
    {
        $processor = new SpySpanProcessor();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        $subscriber = new HttpKernelFlushSubscriber($telemetry, []);

        $event = new TerminateEvent(new StubHttpKernel(), Request::create('/'), new Response());
        $subscriber->onTerminate($event);
        $subscriber->onTerminate($event);

        static::assertSame(2, $processor->flushCount);
        static::assertSame(0, $processor->shutdownCount);
    }
}
