<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\WorkerPollSuppressionSubscriber;
use Flow\Telemetry\Context\MemoryContextStorage;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\Event\WorkerStoppedEvent;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Worker;

use function class_exists;

#[CoversClass(WorkerPollSuppressionSubscriber::class)]
final class WorkerPollSuppressionSubscriberTest extends TestCase
{
    protected function setUp(): void
    {
        if (!class_exists(Worker::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }
    }

    public function test_suppresses_instrumentation_during_the_receive_loop(): void
    {
        $storage = new MemoryContextStorage();
        $subscriber = new WorkerPollSuppressionSubscriber($storage);
        $worker = new Worker([], new MessageBus());

        static::assertFalse($storage->current()->isTracingSuppressed());

        $subscriber->onWorkerStarted(new WorkerStartedEvent($worker));

        static::assertTrue($storage->current()->isTracingSuppressed());
    }

    public function test_resumes_before_the_reset_and_re_suppresses_after(): void
    {
        $storage = new MemoryContextStorage();
        $subscriber = new WorkerPollSuppressionSubscriber($storage);
        $worker = new Worker([], new MessageBus());

        $subscriber->onWorkerStarted(new WorkerStartedEvent($worker));

        $subscriber->onWorkerRunningResume(new WorkerRunningEvent($worker, false));
        static::assertFalse($storage->current()->isTracingSuppressed());

        $subscriber->onWorkerRunningSuppress(new WorkerRunningEvent($worker, false));
        static::assertTrue($storage->current()->isTracingSuppressed());
    }

    public function test_resumes_when_the_worker_stops(): void
    {
        $storage = new MemoryContextStorage();
        $subscriber = new WorkerPollSuppressionSubscriber($storage);
        $worker = new Worker([], new MessageBus());

        $subscriber->onWorkerStarted(new WorkerStartedEvent($worker));
        $subscriber->onWorkerStopped(new WorkerStoppedEvent($worker));

        static::assertFalse($storage->current()->isTracingSuppressed());
    }
}
