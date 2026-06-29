<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\HttpKernelFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Runtime\RuntimeModeResolver;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Runtime\StubWorkerModeDetector;
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
    public function test_classic_mode_shuts_telemetry_down_on_terminate(): void
    {
        $processor = new SpySpanProcessor();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        $subscriber = new HttpKernelFlushSubscriber(
            $telemetry,
            new RuntimeModeResolver('classic', new StubWorkerModeDetector(true)),
            [],
        );

        $subscriber->onTerminate(new TerminateEvent(new StubHttpKernel(), Request::create('/'), new Response()));

        static::assertSame(1, $processor->shutdownCount);
    }

    public function test_worker_mode_flushes_telemetry_without_shutting_down_on_terminate(): void
    {
        $processor = new SpySpanProcessor();
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $telemetry->tracer('test');

        $subscriber = new HttpKernelFlushSubscriber(
            $telemetry,
            new RuntimeModeResolver('worker', new StubWorkerModeDetector(false)),
            [],
        );

        $subscriber->onTerminate(new TerminateEvent(new StubHttpKernel(), Request::create('/'), new Response()));

        static::assertSame(0, $processor->shutdownCount);
        static::assertSame(1, $processor->flushCount);
    }
}
