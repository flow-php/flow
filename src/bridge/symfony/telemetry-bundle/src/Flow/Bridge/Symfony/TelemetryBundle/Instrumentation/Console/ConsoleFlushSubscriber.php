<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console;

use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransport;
use Flow\Telemetry\Telemetry;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

/**
 * Flushes, never shuts down: the command may be nested or inside a worker; shutdown runs at process end.
 */
final readonly class ConsoleFlushSubscriber implements EventSubscriberInterface
{
    /**
     * @param iterable<AsyncCurlTransport> $asyncCurlTransports
     */
    public function __construct(
        private Telemetry $telemetry,
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
        $this->telemetry->flush();

        foreach ($this->asyncCurlTransports as $transport) {
            $transport->tick();
        }
    }
}
