<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\WorkerReceiveCycleSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\Console\Event\ConsoleErrorEvent;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\Event\WorkerStoppedEvent;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Worker;

use function class_exists;

#[CoversClass(WorkerReceiveCycleSubscriber::class)]
final class WorkerReceiveCycleSubscriberTest extends TestCase
{
    protected function setUp(): void
    {
        if (!class_exists(Worker::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }
    }

    public function test_each_worker_pass_produces_one_root_cycle_span(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $subscriber = new WorkerReceiveCycleSubscriber(TelemetryMother::withSpanProcessor($processor));

        $worker = new Worker([], new MessageBus());

        $subscriber->onWorkerStarted(new WorkerStartedEvent($worker));
        $subscriber->completeCycle(new WorkerRunningEvent($worker, false));
        $subscriber->openCycle(new WorkerRunningEvent($worker, false));
        $subscriber->completeCycle(new WorkerRunningEvent($worker, true));
        $subscriber->openCycle(new WorkerRunningEvent($worker, true));
        $subscriber->onWorkerStopped(new WorkerStoppedEvent($worker));

        $cycleSpans = $processor->endedSpans();
        static::assertCount(3, $cycleSpans);

        foreach ($cycleSpans as $span) {
            static::assertSame('messenger.receive', $span->name());
            static::assertSame(SpanKind::CONSUMER, $span->kind());
            static::assertTrue($span->context()->isRoot());
            static::assertSame('symfony_messenger', $span->attributesObject()->get('messaging.system'));
            static::assertSame('receive', $span->attributesObject()->get('messaging.operation.type'));
        }

        static::assertFalse($cycleSpans[0]->attributesObject()->get('messaging.symfony.worker.idle'));
        static::assertTrue($cycleSpans[1]->attributesObject()->get('messaging.symfony.worker.idle'));
    }

    public function test_each_cycle_span_starts_its_own_trace(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $subscriber = new WorkerReceiveCycleSubscriber(TelemetryMother::withSpanProcessor($processor));

        $worker = new Worker([], new MessageBus());

        $subscriber->onWorkerStarted(new WorkerStartedEvent($worker));
        $subscriber->completeCycle(new WorkerRunningEvent($worker, true));
        $subscriber->openCycle(new WorkerRunningEvent($worker, true));
        $subscriber->onWorkerStopped(new WorkerStoppedEvent($worker));

        static::assertCount(2, $processor->traceIds());
    }

    public function test_idle_attribute_is_not_set_on_the_final_stopped_cycle_span(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $subscriber = new WorkerReceiveCycleSubscriber(TelemetryMother::withSpanProcessor($processor));

        $worker = new Worker([], new MessageBus());

        $subscriber->onWorkerStarted(new WorkerStartedEvent($worker));
        $subscriber->onWorkerStopped(new WorkerStoppedEvent($worker));

        $cycleSpans = $processor->endedSpans();
        static::assertCount(1, $cycleSpans);
        static::assertFalse($cycleSpans[0]->attributesObject()->has('messaging.symfony.worker.idle'));
    }

    public function test_cycle_span_carries_transport_names(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $subscriber = new WorkerReceiveCycleSubscriber(TelemetryMother::withSpanProcessor($processor));

        $worker = new Worker(['async' => $this->createStub(ReceiverInterface::class)], new MessageBus());

        $subscriber->onWorkerStarted(new WorkerStartedEvent($worker));
        $subscriber->onWorkerStopped(new WorkerStoppedEvent($worker));

        static::assertSame(
            'async',
            $processor->endedSpans()[0]->attributesObject()->get('messaging.symfony.worker.transports'),
        );
    }

    public function test_completed_cycle_spans_are_flushed_to_the_exporter(): void
    {
        $exporter = new MemoryExporter();
        $subscriber = new WorkerReceiveCycleSubscriber(TelemetryMother::withSpanProcessor(
            new MemorySpanProcessor($exporter),
        ));

        $worker = new Worker([], new MessageBus());

        $subscriber->onWorkerStarted(new WorkerStartedEvent($worker));
        $subscriber->completeCycle(new WorkerRunningEvent($worker, true));

        static::assertCount(1, $exporter->spans());
    }

    public function test_console_error_completes_the_open_cycle_span_with_the_recorded_error(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $subscriber = new WorkerReceiveCycleSubscriber(TelemetryMother::withSpanProcessor($processor));

        $worker = new Worker([], new MessageBus());

        $subscriber->onWorkerStarted(new WorkerStartedEvent($worker));
        $subscriber->onConsoleError(
            new ConsoleErrorEvent(
                new ArrayInput([]),
                new NullOutput(),
                new RuntimeException('database connection lost'),
            ),
        );

        $cycleSpans = $processor->endedSpans();
        static::assertCount(1, $cycleSpans);
        static::assertSame(RuntimeException::class, $cycleSpans[0]->attributesObject()->get('error.type'));
        static::assertTrue($cycleSpans[0]->status()?->isError());
    }

    public function test_console_error_without_an_open_cycle_span_is_a_noop(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $subscriber = new WorkerReceiveCycleSubscriber(TelemetryMother::withSpanProcessor($processor));

        $subscriber->onConsoleError(
            new ConsoleErrorEvent(
                new ArrayInput([]),
                new NullOutput(),
                new RuntimeException('unrelated command failure'),
            ),
        );

        static::assertCount(0, $processor->endedSpans());
    }
}
