<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessageNaming;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\MessageHandler\TestMessageHandler;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Messenger\FailingSender;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\SendMessageMiddleware;
use Symfony\Component\Messenger\Stamp\ConsumedByWorkerStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Transport\InMemory\InMemoryTransport;
use Symfony\Component\Messenger\Transport\Sender\SendersLocator;
use Throwable;

use function interface_exists;

#[CoversClass(TracingMiddleware::class)]
final class TracingMiddlewareTest extends TestCase
{
    protected function setUp(): void
    {
        if (!interface_exists(MiddlewareInterface::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }
    }

    public function test_consumer_emits_consumed_messages_counter(): void
    {
        $memory = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withMetricProcessor($memory);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, null, null, true, true),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello'), [new ReceivedStamp('async')]));
        $telemetry->flush();

        $counters = $memory->metricsWithName('messaging.client.consumed.messages');
        static::assertCount(1, $counters);
        static::assertSame(MetricType::COUNTER, $counters[0]->type);
        static::assertEquals(1, $counters[0]->value);
        static::assertSame('{message}', $counters[0]->unit);
        static::assertSame('process', $counters[0]->attributes->get('messaging.operation.name'));
        static::assertSame('async', $counters[0]->attributes->get('messaging.destination.name'));
        static::assertFalse($counters[0]->attributes->has('messaging.consumer.group.name'));
        static::assertFalse($counters[0]->attributes->has('messaging.message.id'));
        static::assertFalse($counters[0]->attributes->has('flow.messenger.message.class'));
    }

    public function test_consumer_emits_process_duration_histogram(): void
    {
        $memory = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withMetricProcessor($memory);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, null, null, true, true),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello'), [new ReceivedStamp('async')]));
        $telemetry->flush();

        $histograms = $memory->metricsWithName('messaging.process.duration');
        static::assertCount(1, $histograms);
        static::assertSame(MetricType::HISTOGRAM, $histograms[0]->type);
        static::assertSame('s', $histograms[0]->unit);
        static::assertEquals(1, $histograms[0]->attributes->get('histogram.count'));
        static::assertSame(
            [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0],
            $histograms[0]->attributes->get('histogram.explicitBounds'),
        );
    }

    public function test_process_duration_records_error_type_on_failure(): void
    {
        $memory = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withMetricProcessor($memory);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, null, null, true, true),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [static fn(TestMessage $message): never => throw new RuntimeException('boom')],
            ])),
        ]);

        $caught = null;

        try {
            $bus->dispatch(new Envelope(new TestMessage('hello'), [new ReceivedStamp('async')]));
        } catch (Throwable $e) {
            $caught = $e;
        }

        $telemetry->flush();

        static::assertInstanceOf(Throwable::class, $caught);
        $histograms = $memory->metricsWithName('messaging.process.duration');
        static::assertCount(1, $histograms);
        // The actual handler failure, not the HandlerFailedException wrapper every failure shares.
        static::assertSame(RuntimeException::class, $histograms[0]->attributes->get('error.type'));
    }

    public function test_producer_emits_sent_messages_counter(): void
    {
        $memory = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withMetricProcessor($memory);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, null, null, true, true),
            new SendMessageMiddleware(new SendersLocator([TestMessage::class => ['async']], new ServiceLocator([
                'async' => static fn(): InMemoryTransport => new InMemoryTransport(),
            ]))),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello')));
        $telemetry->flush();

        $sent = $memory->metricsWithName('messaging.client.sent.messages');
        static::assertCount(1, $sent);
        static::assertEquals(1, $sent[0]->value);
        static::assertSame('send', $sent[0]->attributes->get('messaging.operation.name'));
        static::assertFalse($sent[0]->attributes->has('messaging.consumer.group.name'));
        static::assertCount(0, $memory->metricsWithName('messaging.client.consumed.messages'));
        static::assertCount(0, $memory->metricsWithName('messaging.process.duration'));
    }

    public function test_producer_span_is_finalized_from_the_sent_stamp(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($processor);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new SendMessageMiddleware(new SendersLocator([TestMessage::class => ['async']], new ServiceLocator([
                'async' => static fn(): InMemoryTransport => new InMemoryTransport(),
            ]))),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello')));
        $telemetry->flush();

        $span = $processor->endedSpans()[0];
        $attributes = $span->attributes();

        // The destination is unknown when the span starts (routing has not happened), but SentStamp and
        // TransportMessageIdStamp are on the envelope once SendMessageMiddleware returns.
        static::assertSame('send async', $span->name());
        static::assertSame('async', $attributes['messaging.destination.name']);
        static::assertArrayHasKey('messaging.message.id', $attributes);
    }

    public function test_sent_counter_records_the_destination(): void
    {
        $memory = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withMetricProcessor($memory);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new SendMessageMiddleware(new SendersLocator([TestMessage::class => ['async']], new ServiceLocator([
                'async' => static fn(): InMemoryTransport => new InMemoryTransport(),
            ]))),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello')));
        $telemetry->flush();

        $sent = $memory->metricsWithName('messaging.client.sent.messages');
        static::assertCount(1, $sent);
        static::assertSame('async', $sent[0]->attributes->get('messaging.destination.name'));
        static::assertFalse($sent[0]->attributes->has('error.type'));
    }

    public function test_sent_counter_records_error_type_when_the_send_fails(): void
    {
        $memory = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withMetricProcessor($memory);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new SendMessageMiddleware(new SendersLocator([TestMessage::class => ['async']], new ServiceLocator([
                'async' => static fn(): FailingSender => new FailingSender(),
            ]))),
        ]);

        $caught = null;

        try {
            $bus->dispatch(new Envelope(new TestMessage('hello')));
        } catch (Throwable $e) {
            $caught = $e;
        }

        $telemetry->flush();

        static::assertInstanceOf(TransportException::class, $caught);

        $sent = $memory->metricsWithName('messaging.client.sent.messages');
        static::assertCount(1, $sent);
        static::assertSame(TransportException::class, $sent[0]->attributes->get('error.type'));
    }

    public function test_sync_handled_message_does_not_count_as_sent(): void
    {
        $memory = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withMetricProcessor($memory);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello')));
        $telemetry->flush();

        static::assertCount(
            0,
            $memory->metricsWithName('messaging.client.sent.messages'),
            'a message handled in-process was never sent to a broker',
        );
    }

    public function test_consumed_counter_records_error_type_on_failure(): void
    {
        $memory = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withMetricProcessor($memory);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [static fn(TestMessage $message): never => throw new RuntimeException('boom')],
            ])),
        ]);

        $caught = null;

        try {
            $bus->dispatch(new Envelope(new TestMessage('hello'), [
                new ReceivedStamp('async'),
                new ConsumedByWorkerStamp(),
            ]));
        } catch (Throwable $e) {
            $caught = $e;
        }

        $telemetry->flush();

        static::assertInstanceOf(Throwable::class, $caught);

        $consumed = $memory->metricsWithName('messaging.client.consumed.messages');
        static::assertCount(1, $consumed);
        static::assertSame(RuntimeException::class, $consumed[0]->attributes->get('error.type'));
    }

    public function test_consumer_failure_records_the_unwrapped_handler_exception(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($processor);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [static fn(TestMessage $message): never => throw new RuntimeException('boom')],
            ])),
        ]);

        $caught = null;

        try {
            $bus->dispatch(new Envelope(new TestMessage('hello'), [
                new ReceivedStamp('async'),
                new ConsumedByWorkerStamp(),
            ]));
        } catch (Throwable $e) {
            $caught = $e;
        }

        $telemetry->flush();

        static::assertInstanceOf(Throwable::class, $caught);

        $span = $processor->endedSpans()[0];

        // HandleMessageMiddleware wraps handler exceptions in HandlerFailedException; the span must record
        // the actual handler failure, not the wrapper every failure shares.
        static::assertSame(RuntimeException::class, $span->attributes()['error.type']);
        static::assertSame('boom', $span->status()?->description);
    }

    public function test_sync_received_message_stays_in_the_current_trace(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $tracer = $telemetry->tracer('app', 'test');

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $outer = $tracer->span('outer');

        // SyncTransport adds ReceivedStamp but not ConsumedByWorkerStamp: handling happens inside the
        // current request, so the process span must stay in the current trace instead of orphaning
        // the handler's work into its own root trace.
        $bus->dispatch(new Envelope(new TestMessage('hello'), [new ReceivedStamp('sync')]));

        $tracer->complete($outer);
        $telemetry->flush();

        $processSpan = $processor->endedSpans()[0];

        static::assertTrue($processSpan->context()->traceId->equals($outer->context()->traceId));
        static::assertSame($outer->context()->spanId->toHex(), $processSpan->context()->parentSpanId?->toHex());
        static::assertCount(0, $processSpan->links());
    }

    public function test_worker_consumed_message_is_a_root_trace(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($processor);
        $tracer = $telemetry->tracer('app', 'test');

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $outer = $tracer->span('outer');

        $bus->dispatch(new Envelope(new TestMessage('hello'), [
            new ReceivedStamp('async'),
            new ConsumedByWorkerStamp(),
        ]));

        $tracer->complete($outer);
        $telemetry->flush();

        $processSpan = $processor->endedSpans()[0];

        static::assertFalse($processSpan->context()->traceId->equals($outer->context()->traceId));
        static::assertNull($processSpan->context()->parentSpanId);
    }

    public function test_spans_are_named_after_the_semconv_destination_by_default(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($processor);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello'), [new ReceivedStamp('async')]));
        $bus->dispatch(new Envelope(new TestMessage('hello')));
        $telemetry->flush();

        $spans = $processor->endedSpans();

        static::assertSame('process async', $spans[0]->name());
        // OTEL messaging semconv: destination unknown at dispatch time, so operation alone.
        static::assertSame('send', $spans[1]->name());
    }

    public function test_message_name_naming_uses_the_message_short_class_name(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($processor);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, null, null, true, true, MessageNaming::MessageName),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello'), [new ReceivedStamp('async')]));
        $bus->dispatch(new Envelope(new TestMessage('hello')));
        $telemetry->flush();

        $spans = $processor->endedSpans();

        static::assertSame('process TestMessage', $spans[0]->name());
        static::assertSame('send TestMessage', $spans[1]->name());
    }

    public function test_message_fqcn_naming_uses_the_fully_qualified_class_name(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($processor);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, null, null, true, true, MessageNaming::MessageFqcn),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello')));
        $telemetry->flush();

        static::assertSame('send ' . TestMessage::class, $processor->endedSpans()[0]->name());
    }

    public function test_no_metrics_emitted_when_disabled(): void
    {
        $memory = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withMetricProcessor($memory);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, null, null, true, false),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello'), [new ReceivedStamp('async')]));
        $telemetry->flush();

        static::assertSame(0, $memory->countMetrics());
    }

    public function test_no_span_emitted_when_handler_tracing_disabled(): void
    {
        $processor = new MemorySpanProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withSpanProcessor($processor);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, null, null, false),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello'), [new ReceivedStamp('async')]));
        $telemetry->flush();

        static::assertCount(0, $processor->endedSpans());
    }

    public function test_metrics_emitted_without_a_handler_span(): void
    {
        $memory = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withMetricProcessor($memory);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, null, null, false, true),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello'), [new ReceivedStamp('async')]));
        $telemetry->flush();

        static::assertCount(1, $memory->metricsWithName('messaging.client.consumed.messages'));
        static::assertCount(1, $memory->metricsWithName('messaging.process.duration'));
    }
}
