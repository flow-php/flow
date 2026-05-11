<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

use Flow\Telemetry\Telemetry;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpKernel\Event\TerminateEvent;
use Symfony\Component\HttpKernel\KernelEvents;

final readonly class HttpKernelFlushSubscriber implements EventSubscriberInterface
{
    public function __construct(
        private Telemetry $telemetry,
    ) {}

    public static function getSubscribedEvents(): array
    {
        return [
            KernelEvents::TERMINATE => ['onTerminate', -20000],
        ];
    }

    public function onTerminate(TerminateEvent $event): void
    {
        $this->telemetry->shutdown();
    }
}
