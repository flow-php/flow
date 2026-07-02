<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Profiler;

use Flow\Bridge\Symfony\TelemetryBundle\Profiler\ProfilerStoreWorkerResetSubscriber;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tests\Mother\SpanMother;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Worker;

use function class_exists;

#[CoversClass(ProfilerStoreWorkerResetSubscriber::class)]
final class ProfilerStoreWorkerResetSubscriberTest extends TestCase
{
    protected function setUp(): void
    {
        if (!class_exists(Worker::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }
    }

    public function test_it_empties_the_store_after_a_worker_cycle(): void
    {
        $store = new MemoryExporter();
        $store->export(Signals::traces([SpanMother::withName('span')]));
        static::assertCount(1, $store->spans());

        $subscriber = new ProfilerStoreWorkerResetSubscriber($store);
        $subscriber->onWorkerRunning(new WorkerRunningEvent(new Worker([], new MessageBus()), false));

        static::assertSame([], $store->spans());
    }

    public function test_it_subscribes_to_the_worker_running_event_after_the_cycle_flush(): void
    {
        // the worker flushes signals into the store; resetting must run
        // afterwards (priority 512 < 1024) so the just-flushed signals are cleared and never accumulate.
        static::assertSame(
            [WorkerRunningEvent::class => ['onWorkerRunning', 512]],
            ProfilerStoreWorkerResetSubscriber::getSubscribedEvents(),
        );
    }
}
