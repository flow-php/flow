<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Profiler;

use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;

/**
 * Workers have no request cycle, so the store is reset per message or it grows unbounded.
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
