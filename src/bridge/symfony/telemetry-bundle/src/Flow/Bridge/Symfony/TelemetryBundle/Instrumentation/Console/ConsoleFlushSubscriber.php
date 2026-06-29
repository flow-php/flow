<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Runtime\RuntimeModeResolver;
use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransport;
use Flow\Telemetry\Telemetry;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

final readonly class ConsoleFlushSubscriber implements EventSubscriberInterface
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
            ConsoleEvents::TERMINATE => ['onTerminate', -20000],
        ];
    }

    public function onTerminate(ConsoleTerminateEvent $event): void
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
