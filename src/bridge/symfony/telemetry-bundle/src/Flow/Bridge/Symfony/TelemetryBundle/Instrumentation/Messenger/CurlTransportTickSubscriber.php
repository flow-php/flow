<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransport;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;

final readonly class CurlTransportTickSubscriber implements EventSubscriberInterface
{
    /**
     * @param iterable<CurlTransport> $transports
     */
    public function __construct(
        private iterable $transports,
    ) {}

    public static function getSubscribedEvents(): array
    {
        return [
            WorkerRunningEvent::class => 'onWorkerRunning',
        ];
    }

    public function onWorkerRunning(WorkerRunningEvent $event): void
    {
        foreach ($this->transports as $transport) {
            $transport->tick();
        }
    }
}
