<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger;

use Flow\Telemetry\Telemetry;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Event\WorkerMessageHandledEvent;

/**
 * Drains buffered telemetry after each consumed message.
 */
final readonly class MessengerFlushSubscriber implements EventSubscriberInterface
{
    public function __construct(
        private Telemetry $telemetry,
    ) {}

    public static function getSubscribedEvents(): array
    {
        return [
            WorkerMessageHandledEvent::class => ['onMessageHandled', -20000],
            WorkerMessageFailedEvent::class => ['onMessageFailed', -20000],
        ];
    }

    public function onMessageHandled(WorkerMessageHandledEvent $event): void
    {
        $this->telemetry->flush();
    }

    public function onMessageFailed(WorkerMessageFailedEvent $event): void
    {
        $this->telemetry->flush();
    }
}
