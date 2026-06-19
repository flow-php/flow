<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Logger\ConsoleOutputLogProcessor;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;

final readonly class ConsoleLogOutputSubscriber implements EventSubscriberInterface
{
    public function __construct(
        private ConsoleOutputLogProcessor $processor,
    ) {}

    public static function getSubscribedEvents(): array
    {
        return [
            ConsoleEvents::COMMAND => ['onCommand', 9000],
            ConsoleEvents::TERMINATE => ['onTerminate', -30000],
        ];
    }

    public function onCommand(ConsoleCommandEvent $event): void
    {
        $this->processor->setOutput($event->getOutput());
    }

    public function onTerminate(ConsoleTerminateEvent $event): void
    {
        $this->processor->clearOutput();
    }
}
