<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache;

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
 * Symfony cache pools defer writes and flush them from the pool's own commit path at object destruction,
 * outside any request/message span. On a Doctrine DBAL pool each of those writes therefore surfaces as an
 * orphan doctrine.dbal.* span (statement.prepare/execute, transaction.begin/commit), one trace per query.
 *
 * This subscriber drains the deferred writes at a controlled point — request/command termination and after
 * each consumed message — inside a single "cache.flush" span, so the writes group under one trace instead of
 * orphaning, and the pool has nothing left to flush at destruction. It commits the inner (untraced) adapters
 * so idle pools do not emit empty per-pool cache spans; only real DBAL work nests under the flush span. When
 * the surrounding context suppresses tracing (e.g. a messenger worker's receive loop), the flush is silently
 * non-recording while still draining the queue.
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
        $span = $tracer->span('cache.flush', SpanKind::INTERNAL, ['cache.operation' => 'flush']);

        try {
            foreach ($this->pools as $pool) {
                $pool->commit();
            }
        } finally {
            $tracer->complete($span);
        }
    }
}
