<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Tracer\Processor\BatchingSpanProcessor;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\Event\WorkerMessageHandledEvent;

#[CoversClass(MessengerFlushSubscriber::class)]
final class MessengerFlushSubscriberTest extends TestCase
{
    public function test_flush_on_handled_exports_buffered_span(): void
    {
        $exporter = new MemoryExporter();
        $telemetry = TelemetryMother::withSpanProcessor(new BatchingSpanProcessor($exporter, 512));
        $tracer = $telemetry->tracer('test');
        $tracer->complete($tracer->span('op'));

        static::assertCount(0, $exporter->spans());

        (new MessengerFlushSubscriber($telemetry))->onMessageHandled(
            new WorkerMessageHandledEvent(new Envelope(new stdClass()), 'receiver'),
        );

        static::assertCount(1, $exporter->spans());
    }

    public function test_flush_is_not_terminal_subsequent_messages_still_export(): void
    {
        $exporter = new MemoryExporter();
        $telemetry = TelemetryMother::withSpanProcessor(new BatchingSpanProcessor($exporter, 512));
        $subscriber = new MessengerFlushSubscriber($telemetry);
        $tracer = $telemetry->tracer('test');

        $tracer->complete($tracer->span('first'));
        $subscriber->onMessageHandled(new WorkerMessageHandledEvent(new Envelope(new stdClass()), 'receiver'));
        static::assertCount(1, $exporter->spans());

        $tracer->complete($tracer->span('second'));
        $subscriber->onMessageHandled(new WorkerMessageHandledEvent(new Envelope(new stdClass()), 'receiver'));
        static::assertCount(2, $exporter->spans());
    }

    public function test_flush_on_failed_exports_buffered_span(): void
    {
        $exporter = new MemoryExporter();
        $telemetry = TelemetryMother::withSpanProcessor(new BatchingSpanProcessor($exporter, 512));
        $tracer = $telemetry->tracer('test');
        $tracer->complete($tracer->span('op'));

        static::assertCount(0, $exporter->spans());

        (new MessengerFlushSubscriber($telemetry))->onMessageFailed(
            new WorkerMessageFailedEvent(new Envelope(new stdClass()), 'receiver', new RuntimeException('boom')),
        );

        static::assertCount(1, $exporter->spans());
    }

    public function test_subscribed_events(): void
    {
        static::assertSame(
            [
                WorkerMessageHandledEvent::class => ['onMessageHandled', -20000],
                WorkerMessageFailedEvent::class => ['onMessageFailed', -20000],
            ],
            MessengerFlushSubscriber::getSubscribedEvents(),
        );
    }
}
