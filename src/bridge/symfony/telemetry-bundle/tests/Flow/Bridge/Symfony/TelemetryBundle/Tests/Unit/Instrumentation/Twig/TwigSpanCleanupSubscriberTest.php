<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Twig;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Twig\TracingTwigExtension;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Twig\TwigSpanCleanupSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Event\TerminateEvent;
use Symfony\Component\HttpKernel\HttpKernelInterface;
use Symfony\Component\HttpKernel\KernelEvents;
use Twig\Profiler\Profile;

#[CoversClass(TwigSpanCleanupSubscriber::class)]
final class TwigSpanCleanupSubscriberTest extends TestCase
{
    public function test_subscribes_to_kernel_and_console_terminate_before_flush(): void
    {
        $events = TwigSpanCleanupSubscriber::getSubscribedEvents();

        static::assertArrayHasKey(KernelEvents::TERMINATE, $events);
        static::assertArrayHasKey(ConsoleEvents::TERMINATE, $events);
        static::assertSame(['onTerminate', -15000], $events[KernelEvents::TERMINATE]);
        static::assertSame(['onConsoleTerminate', -15000], $events[ConsoleEvents::TERMINATE]);
    }

    public function test_on_console_terminate_completes_orphaned_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $extension = new TracingTwigExtension(TelemetryMother::withSpanProcessor($spanProcessor), traceTemplates: true);

        $extension->enter(new Profile('templates/home.html.twig', Profile::TEMPLATE, 'templates/home.html.twig'));

        $subscriber = new TwigSpanCleanupSubscriber($extension);
        $command = new Command('app:process');
        $subscriber->onConsoleTerminate(new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0));

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
    }

    public function test_on_terminate_completes_orphaned_span(): void
    {
        $spanProcessor = new MemorySpanProcessor(new MemoryExporter());
        $extension = new TracingTwigExtension(TelemetryMother::withSpanProcessor($spanProcessor), traceTemplates: true);

        $extension->enter(new Profile('templates/home.html.twig', Profile::TEMPLATE, 'templates/home.html.twig'));

        $subscriber = new TwigSpanCleanupSubscriber($extension);
        $kernel = $this->createStub(HttpKernelInterface::class);
        $subscriber->onTerminate(new TerminateEvent($kernel, new Request(), new Response()));

        $spans = $spanProcessor->endedSpans();
        static::assertCount(1, $spans);

        $status = $spans[0]->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
    }
}
