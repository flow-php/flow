<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\MessageHandler\TestMessageHandler;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\TelemetryMother;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Flow\Telemetry\Provider\Memory\MemoryMetricProcessor;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
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
        static::assertSame($caught::class, $histograms[0]->attributes->get('error.type'));
    }

    public function test_producer_emits_sent_messages_counter(): void
    {
        $memory = new MemoryMetricProcessor(new MemoryExporter());
        $telemetry = TelemetryMother::withMetricProcessor($memory);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, null, null, true, true),
            new HandleMessageMiddleware(new HandlersLocator([TestMessage::class => [new TestMessageHandler()]])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('hello')));
        $telemetry->flush();

        $sent = $memory->metricsWithName('messaging.client.sent.messages');
        static::assertCount(1, $sent);
        static::assertEquals(1, $sent[0]->value);
        static::assertSame('send', $sent[0]->attributes->get('messaging.operation.name'));
        static::assertFalse($sent[0]->attributes->has('messaging.destination.name'));
        static::assertFalse($sent[0]->attributes->has('messaging.consumer.group.name'));
        static::assertCount(0, $memory->metricsWithName('messaging.client.consumed.messages'));
        static::assertCount(0, $memory->metricsWithName('messaging.process.duration'));
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
