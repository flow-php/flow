<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console;

use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransport;
use Flow\Telemetry\Telemetry;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

/**
 * Drains buffered telemetry when a console command terminates.
 *
 * Terminate is only a flush point - it never shuts telemetry down, because
 * the terminating command is not necessarily the last work of the process:
 * it may be a command nested inside another command via Application::run(),
 * or a message handler's sub-command inside a long-running messenger worker.
 * Transport shutdown happens once, at real process end, through the shutdown
 * function registered by Telemetry::registerShutdownFunction().
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
