<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Twig;

use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpKernel\Event\TerminateEvent;
use Symfony\Component\HttpKernel\KernelEvents;

/**
 * Completes Twig spans left open by a render exception and resets the extension state.
 */
final readonly class TwigSpanCleanupSubscriber implements EventSubscriberInterface
{
    public function __construct(
        private TracingTwigExtension $extension,
    ) {}

    public static function getSubscribedEvents(): array
    {
        return [
            KernelEvents::TERMINATE => ['onTerminate', -15000],
            ConsoleEvents::TERMINATE => ['onConsoleTerminate', -15000],
        ];
    }

    public function onConsoleTerminate(ConsoleTerminateEvent $event): void
    {
        $this->extension->reset();
    }

    public function onTerminate(TerminateEvent $event): void
    {
        $this->extension->reset();
    }
}
