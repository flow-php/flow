<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache;

use Flow\Telemetry\CacheAttributes;
use Flow\Telemetry\PackageVersion;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use Psr\Cache\CacheItemPoolInterface;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpKernel\KernelEvents;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Event\WorkerMessageHandledEvent;

use function class_exists;

/**
 * Pools flush at destruction outside any span; draining here groups DBAL writes under one cache.flush span.
 */
final readonly class CacheDeferredFlushSubscriber implements EventSubscriberInterface
{
    private const FLUSH_PRIORITY = -15000;

    /**
     * @param iterable<CacheItemPoolInterface> $pools
     */
    public function __construct(
        private iterable $pools,
        private Telemetry $telemetry,
    ) {}

    public static function getSubscribedEvents(): array
    {
        $events = [
            KernelEvents::TERMINATE => ['onFlush', self::FLUSH_PRIORITY],
        ];

        if (class_exists(ConsoleEvents::class)) {
            $events[ConsoleEvents::TERMINATE] = ['onFlush', self::FLUSH_PRIORITY];
        }

        if (class_exists(WorkerMessageHandledEvent::class)) {
            $events[WorkerMessageHandledEvent::class] = ['onFlush', self::FLUSH_PRIORITY];
            $events[WorkerMessageFailedEvent::class] = ['onFlush', self::FLUSH_PRIORITY];
        }

        return $events;
    }

    public function onFlush(object $event): void
    {
        $tracer = $this->telemetry->tracer('flow.symfony.cache', PackageVersion::get('symfony/cache'));
        $span = $tracer->span('cache.flush', SpanKind::INTERNAL, [CacheAttributes::CACHE_OPERATION => 'flush']);
        $scope = $tracer->activate($span);

        try {
            foreach ($this->pools as $pool) {
                $pool->commit();
            }
        } finally {
            $scope->detach();
            $tracer->complete($span);
        }
    }
}
