<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Cache;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\CacheDeferredFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\TraceableCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Cache\ArrayCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use stdClass;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\HttpKernel\KernelEvents;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Event\WorkerMessageHandledEvent;

use function class_exists;

#[CoversClass(CacheDeferredFlushSubscriber::class)]
final class CacheDeferredFlushSubscriberTest extends TestCase
{
    public function test_it_commits_deferred_writes_of_every_pool(): void
    {
        $telemetry = TelemetryMother::withSpanProcessor(new MemorySpanProcessor(new MemoryExporter()));

        $poolA = new ArrayCacheAdapter();
        $poolB = new ArrayCacheAdapter();

        $itemA = $poolA->getItem('a');
        $itemA->set('value');
        $poolA->saveDeferred($itemA);

        $itemB = $poolB->getItem('b');
        $itemB->set('value');
        $poolB->saveDeferred($itemB);

        (new CacheDeferredFlushSubscriber([$poolA, $poolB], $telemetry))->onFlush(new stdClass());

        static::assertTrue($poolA->hasItem('a'));
        static::assertTrue($poolB->hasItem('b'));
    }

    public function test_it_wraps_the_flush_in_a_single_cache_flush_span(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($processor);

        (new CacheDeferredFlushSubscriber([new ArrayCacheAdapter()], $telemetry))->onFlush(new stdClass());

        $spans = $processor->endedSpans();
        static::assertCount(1, $spans);
        static::assertSame('cache.flush', $spans[0]->name());
    }

    public function test_pool_writes_are_nested_under_the_flush_span(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($processor);

        $pool = new TraceableCacheAdapter(new ArrayCacheAdapter(), $telemetry, 'cache.test');
        $item = $pool->getItem('k');
        $item->set('value');
        $pool->saveDeferred($item);

        (new CacheDeferredFlushSubscriber([$pool], $telemetry))->onFlush(new stdClass());

        $byName = [];

        foreach ($processor->endedSpans() as $span) {
            $byName[$span->name()] = $span;
        }

        static::assertArrayHasKey('cache.flush', $byName);
        static::assertArrayHasKey('cache.commit', $byName);
        static::assertSame(
            $byName['cache.flush']->context()->normalize()['spanId']['hex'],
            $byName['cache.commit']->context()->normalize()['parentSpanId']['hex'] ?? null,
        );
    }

    public function test_it_subscribes_to_termination_and_message_events_below_the_telemetry_flush(): void
    {
        $events = CacheDeferredFlushSubscriber::getSubscribedEvents();

        static::assertSame(['onFlush', -15000], $events[KernelEvents::TERMINATE]);
        static::assertSame(['onFlush', -15000], $events[ConsoleEvents::TERMINATE]);

        if (class_exists(WorkerMessageHandledEvent::class)) {
            static::assertSame(['onFlush', -15000], $events[WorkerMessageHandledEvent::class]);
            static::assertSame(['onFlush', -15000], $events[WorkerMessageFailedEvent::class]);
        }
    }
}
