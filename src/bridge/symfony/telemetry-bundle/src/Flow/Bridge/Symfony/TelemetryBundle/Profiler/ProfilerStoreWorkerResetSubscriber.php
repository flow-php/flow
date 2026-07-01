<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Profiler;

use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;

/**
 * The profiler store accumulates every signal teed into it and is normally emptied by the data collector
 * between HTTP requests. A messenger worker is a single long-lived process with no request cycle, so the
 * collector never resets the store and it grows until the process runs out of memory. Resetting it after
 * each worker cycle bounds it to a single message, mirroring how Symfony resets data collectors per message.
 *
 * The event only fires when symfony/messenger is installed; otherwise this subscriber is inert.
 */
final readonly class ProfilerStoreWorkerResetSubscriber implements EventSubscriberInterface
{
    private const RESET_PRIORITY = 512;

    public function __construct(
        private MemoryExporter $store,
    ) {}

    public static function getSubscribedEvents(): array
    {
        return [
            WorkerRunningEvent::class => ['onWorkerRunning', self::RESET_PRIORITY],
        ];
    }

    public function onWorkerRunning(WorkerRunningEvent $event): void
    {
        $this->store->reset();
    }
}
