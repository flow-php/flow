<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerHandlerLink;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\WorkerReceiveCycleSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\MessageHandler\TestMessageHandler;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Propagation\Propagator;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use RuntimeException;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleErrorEvent;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\Event\WorkerStoppedEvent;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Worker;

use function interface_exists;

#[CoversClass(WorkerReceiveCycleSubscriber::class)]
final class WorkerReceiveCycleSubscriberTest extends KernelTestCase
{
    protected function setUp(): void
    {
        if (!interface_exists(MiddlewareInterface::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }

        parent::setUp();
    }

    public function test_transport_poll_queries_group_under_one_cycle_trace_and_consumer_links_to_it(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'propagator' => ['type' => 'w3c'],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => ['enabled' => true, 'context_propagation' => true],
                    ],
                ]);
            },
        ]);

        $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [new TestMessageHandler()],
            ])),
        ]);

        $dispatcher = new EventDispatcher();
        $dispatcher->addSubscriber(new WorkerReceiveCycleSubscriber($telemetry));
        $worker = new Worker([], $bus, $dispatcher);

        $dispatcher->dispatch(new WorkerStartedEvent($worker));

        // Simulate the Doctrine transport's claim query running inside the worker's receive loop,
        // outside any message-handling span.
        $dbalTracer = $telemetry->tracer('flow.symfony.dbal', 'test');
        $dbalTracer->complete($dbalTracer->span('doctrine.dbal.transaction.begin'));

        $bus->dispatch(new Envelope(new TestMessage('a'), [new ReceivedStamp('async')]));

        $dispatcher->dispatch(new WorkerRunningEvent($worker, false));
        $dispatcher->dispatch(new WorkerStoppedEvent($worker));

        $processor = $this->symfonyContext()->getService(
            'flow.telemetry.tracer_provider.processor',
            MemorySpanProcessor::class,
        );

        $receiveSpan = null;
        $pollSpan = null;
        $consumerSpan = null;

        foreach ($processor->endedSpans() as $span) {
            if (
                $span->name() === 'messenger.receive'
                && $span->attributesObject()->get('messaging.symfony.worker.idle') === false
            ) {
                $receiveSpan = $span;
            }

            if ($span->name() === 'doctrine.dbal.transaction.begin') {
                $pollSpan = $span;
            }

            if ($span->kind() === SpanKind::CONSUMER && $span->name() === 'process TestMessage') {
                $consumerSpan = $span;
            }
        }

        static::assertNotNull($receiveSpan);
        static::assertNotNull($pollSpan);
        static::assertNotNull($consumerSpan);

        static::assertSame(
            $receiveSpan->context()->traceId->toHex(),
            $pollSpan->context()->traceId->toHex(),
            'transport poll query should nest under the receive-cycle trace',
        );
        static::assertSame(
            $receiveSpan->context()->spanId->toHex(),
            $pollSpan->context()->parentSpanId?->toHex(),
            'transport poll query should be a child of the receive-cycle span',
        );

        static::assertNotSame(
            $receiveSpan->context()->traceId->toHex(),
            $consumerSpan->context()->traceId->toHex(),
            'consumed message remains its own root trace',
        );

        $workerLinkTraceIds = [];

        foreach ($consumerSpan->links() as $link) {
            if ($link->attributes->get('flow.messenger.worker') === true) {
                $workerLinkTraceIds[] = $link->context->traceId->toHex();
            }
        }

        static::assertSame(
            [$receiveSpan->context()->traceId->toHex()],
            $workerLinkTraceIds,
            'consumer span should link back to its receive-cycle trace',
        );
    }

    public function test_console_error_keeps_the_poll_trace_whole_when_the_transport_crashes(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'propagator' => ['type' => 'w3c'],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => ['enabled' => true, 'context_propagation' => true],
                    ],
                ]);
            },
        ]);

        $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);

        $dispatcher = new EventDispatcher();
        $dispatcher->addSubscriber(new WorkerReceiveCycleSubscriber($telemetry));
        $worker = new Worker([], new MessageBus(), $dispatcher);

        $dispatcher->dispatch(new WorkerStartedEvent($worker));

        // The transport's claim query runs under the cycle span, then the poll itself throws.
        $dbalTracer = $telemetry->tracer('flow.symfony.dbal', 'test');
        $dbalTracer->complete($dbalTracer->span('doctrine.dbal.transaction.begin'));

        $dispatcher->dispatch(
            new ConsoleErrorEvent(new ArrayInput([]), new NullOutput(), new RuntimeException('connection lost')),
            ConsoleEvents::ERROR,
        );

        static::assertNull($contextStorage->current()->activeSpan(), 'cycle span must be detached after the crash');

        $processor = $this->symfonyContext()->getService(
            'flow.telemetry.tracer_provider.processor',
            MemorySpanProcessor::class,
        );

        $receiveSpan = null;
        $pollSpan = null;

        foreach ($processor->endedSpans() as $span) {
            if ($span->name() === 'messenger.receive') {
                $receiveSpan = $span;
            }

            if ($span->name() === 'doctrine.dbal.transaction.begin') {
                $pollSpan = $span;
            }
        }

        static::assertNotNull($receiveSpan);
        static::assertNotNull($pollSpan);

        static::assertTrue($receiveSpan->status()?->isError(), 'cycle span records the crash');
        static::assertSame(RuntimeException::class, $receiveSpan->attributesObject()->get('error.type'));
        static::assertSame(
            $receiveSpan->context()->spanId->toHex(),
            $pollSpan->context()->parentSpanId?->toHex(),
            'the claim query keeps its root instead of becoming a rootless trace',
        );
    }

    public function test_link_dispatcher_omits_the_worker_link_even_with_an_active_cycle_span(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => 'memory',
                        ],
                    ],
                    'propagator' => ['type' => 'w3c'],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => ['enabled' => true, 'context_propagation' => true],
                    ],
                ]);
            },
        ]);

        $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator, true, MessengerHandlerLink::Dispatcher),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [new TestMessageHandler()],
            ])),
        ]);

        $dispatcher = new EventDispatcher();
        $dispatcher->addSubscriber(new WorkerReceiveCycleSubscriber($telemetry));
        $worker = new Worker([], $bus, $dispatcher);

        $dispatcher->dispatch(new WorkerStartedEvent($worker));
        $bus->dispatch(new Envelope(new TestMessage('a'), [new ReceivedStamp('async')]));
        $dispatcher->dispatch(new WorkerStoppedEvent($worker));

        $processor = $this->symfonyContext()->getService(
            'flow.telemetry.tracer_provider.processor',
            MemorySpanProcessor::class,
        );

        $consumerSpan = null;

        foreach ($processor->endedSpans() as $span) {
            if ($span->kind() === SpanKind::CONSUMER && $span->name() === 'process TestMessage') {
                $consumerSpan = $span;
            }
        }

        static::assertNotNull($consumerSpan);

        foreach ($consumerSpan->links() as $link) {
            static::assertNotSame(true, $link->attributes->get('flow.messenger.worker'));
        }
    }
}
