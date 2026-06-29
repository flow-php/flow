<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel;

use Flow\Bridge\Symfony\TelemetryBundle\Runtime\RuntimeModeResolver;
use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransport;
use Flow\Telemetry\Telemetry;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpKernel\Event\TerminateEvent;
use Symfony\Component\HttpKernel\KernelEvents;

final readonly class HttpKernelFlushSubscriber implements EventSubscriberInterface
{
    /**
     * @param iterable<AsyncCurlTransport> $asyncCurlTransports
     */
    public function __construct(
        private Telemetry $telemetry,
        private RuntimeModeResolver $runtimeMode,
        private iterable $asyncCurlTransports,
    ) {}

    public static function getSubscribedEvents(): array
    {
        return [
            KernelEvents::TERMINATE => ['onTerminate', -20000],
        ];
    }

    public function onTerminate(TerminateEvent $event): void
    {
        if (!$this->runtimeMode->isWorker()) {
            $this->telemetry->shutdown();

            return;
        }

        $this->telemetry->flush();

        foreach ($this->asyncCurlTransports as $transport) {
            $transport->tick();
        }
    }
}
