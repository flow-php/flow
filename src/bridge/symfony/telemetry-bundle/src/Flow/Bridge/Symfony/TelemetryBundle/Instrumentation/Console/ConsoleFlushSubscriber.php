<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console;

use Flow\Telemetry\Telemetry;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

final readonly class ConsoleFlushSubscriber implements EventSubscriberInterface
{
    public function __construct(
        private Telemetry $telemetry,
    ) {
    }

    public static function getSubscribedEvents() : array
    {
        return [
            ConsoleEvents::TERMINATE => ['onTerminate', -20000],
        ];
    }

    public function onTerminate(ConsoleTerminateEvent $event) : void
    {
        $this->telemetry->shutdown();
    }
}
