<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerTracePropagation;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TelemetryStamp;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\MessageHandler\TestMessageHandler;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Middleware\CapturingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\TraceId;
use Flow\Telemetry\Propagation\Propagator;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use RuntimeException;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Stamp\BusNameStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Throwable;

use function interface_exists;

#[CoversClass(TracingMiddleware::class)]
final class TracingMiddlewareTest extends KernelTestCase
{
    protected function setUp(): void
    {
        if (!interface_exists(MiddlewareInterface::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }

        parent::setUp();
    }

    public function test_context_is_extracted_on_consume(): void
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
                        'messenger' => [
                            'enabled' => true,
                            'context_propagation' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $originalTraceId = 'abcdef0123456789abcdef0123456789';
        $originalSpanId = '0123456789abcdef';
        $traceparent = "00-{$originalTraceId}-{$originalSpanId}-01";

        $telemetryStamp = new TelemetryStamp(['traceparent' => $traceparent]);

        $handler = new TestMessageHandler();

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator, MessengerTracePropagation::Continuation),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [$handler],
            ])),
        ]);

        $envelope = new Envelope(new TestMessage('test'), [
            new ReceivedStamp('async'),
            $telemetryStamp,
        ]);

        $bus->dispatch($envelope);

        $processor = $this->symfonyContext()->getService(
            'flow.telemetry.tracer_provider.processor',
            MemorySpanProcessor::class,
        );
        $spans = $processor->endedSpans();

        static::assertCount(1, $spans);
        $span = $spans[0];

        static::assertSame(SpanKind::CONSUMER, $span->kind());
        static::assertSame($originalTraceId, $span->context()->traceId->toHex());
    }

    public function test_consumer_span_has_no_links_in_continue_mode(): void
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
                        'messenger' => [
                            'enabled' => true,
                            'context_propagation' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $originalTraceId = 'abcdef0123456789abcdef0123456789';
        $originalSpanId = '0123456789abcdef';
        $traceparent = "00-{$originalTraceId}-{$originalSpanId}-01";

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator, MessengerTracePropagation::Continuation),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [new TestMessageHandler()],
            ])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('test'), [
            new ReceivedStamp('async'),
            new TelemetryStamp(['traceparent' => $traceparent]),
        ]));

        $processor = $this->symfonyContext()->getService(
            'flow.telemetry.tracer_provider.processor',
            MemorySpanProcessor::class,
        );
        $spans = $processor->endedSpans();

        static::assertCount(1, $spans);
        $span = $spans[0];

        static::assertSame(SpanKind::CONSUMER, $span->kind());
        static::assertSame($originalTraceId, $span->context()->traceId->toHex());
        static::assertCount(0, $span->links());
    }

    public function test_consumer_span_links_to_producer_in_link_mode(): void
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
                        'messenger' => [
                            'enabled' => true,
                            'context_propagation' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $originalTraceId = 'abcdef0123456789abcdef0123456789';
        $originalSpanId = '0123456789abcdef';
        $traceparent = "00-{$originalTraceId}-{$originalSpanId}-01";

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator, MessengerTracePropagation::Link),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [new TestMessageHandler()],
            ])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('test'), [
            new ReceivedStamp('async'),
            new TelemetryStamp(['traceparent' => $traceparent]),
        ]));

        $processor = $this->symfonyContext()->getService(
            'flow.telemetry.tracer_provider.processor',
            MemorySpanProcessor::class,
        );
        $spans = $processor->endedSpans();

        static::assertCount(1, $spans);
        $span = $spans[0];

        static::assertSame(SpanKind::CONSUMER, $span->kind());
        static::assertNotSame($originalTraceId, $span->context()->traceId->toHex());

        $links = $span->links();
        static::assertCount(1, $links);

        $linkedContext = $links[0]->context;
        static::assertSame($originalTraceId, $linkedContext->traceId->toHex());
        static::assertSame($originalSpanId, $linkedContext->spanId->toHex());
        static::assertTrue($linkedContext->isRemote);
    }

    public function test_consumer_attaches_producer_baggage_in_link_mode(): void
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
                        'messenger' => [
                            'enabled' => true,
                            'context_propagation' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $traceparent = '00-abcdef0123456789abcdef0123456789-0123456789abcdef-01';

        // Link mode runs the consumer span under the worker's own trace (e.g. the
        // messenger:consume console span); establish that active worker trace here.
        $workerTracer = $telemetry->tracer('worker');
        $workerSpan = $workerTracer->span('messenger:consume');

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator, MessengerTracePropagation::Link),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [new TestMessageHandler()],
            ])),
        ]);

        $bus->dispatch(new Envelope(new TestMessage('test'), [
            new ReceivedStamp('async'),
            new TelemetryStamp(['traceparent' => $traceparent, 'baggage' => 'user.id=42']),
        ]));

        $baggage = $contextStorage->current()->baggage;
        static::assertFalse($baggage->isEmpty());
        static::assertSame('42', $baggage->get('user.id'));

        $workerTracer->complete($workerSpan);
    }

    public function test_producer_injects_stamp_in_link_mode(): void
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
                        'messenger' => [
                            'enabled' => true,
                            'context_propagation' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $traceId = TraceId::generate();
        $context = Context::withTraceId($traceId);
        $contextStorage->attach($context);

        $tracer = $telemetry->tracer('test');
        $span = $tracer->span('parent-span');
        $contextStorage->attach($context->withActiveSpan($span->context()->spanId));

        $capturingMiddleware = new CapturingMiddleware();

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator, MessengerTracePropagation::Link),
            $capturingMiddleware,
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [new TestMessageHandler()],
            ])),
        ]);

        $bus->dispatch(new TestMessage('test'));

        $tracer->complete($span);

        static::assertNotNull($capturingMiddleware->captured);
        $stamp = $capturingMiddleware->captured->last(TelemetryStamp::class);
        static::assertInstanceOf(TelemetryStamp::class, $stamp);

        $traceparent = $stamp->get('traceparent');
        static::assertNotNull($traceparent);
        static::assertStringContainsString($traceId->toHex(), $traceparent);
    }

    public function test_context_is_injected_on_dispatch_when_context_propagation_enabled(): void
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
                        'messenger' => [
                            'enabled' => true,
                            'context_propagation' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $traceId = TraceId::generate();
        $context = Context::withTraceId($traceId);
        $contextStorage->attach($context);

        $tracer = $telemetry->tracer('test');
        $span = $tracer->span('parent-span');
        $context = $context->withActiveSpan($span->context()->spanId);
        $contextStorage->attach($context);

        $handler = new TestMessageHandler();
        $capturingMiddleware = new CapturingMiddleware();

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator),
            $capturingMiddleware,
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [$handler],
            ])),
        ]);

        $bus->dispatch(new TestMessage('test'));

        $tracer->complete($span);

        static::assertNotNull($capturingMiddleware->captured);
        $stamp = $capturingMiddleware->captured->last(TelemetryStamp::class);
        static::assertInstanceOf(TelemetryStamp::class, $stamp);

        $traceparent = $stamp->get('traceparent');
        static::assertNotNull($traceparent);
        static::assertStringContainsString($traceId->toHex(), $traceparent);
    }

    public function test_context_propagation_not_applied_when_disabled(): void
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
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => [
                            'enabled' => true,
                            'context_propagation' => false,
                        ],
                    ],
                ]);
            },
        ]);

        $middleware = $this->symfonyContext()->getService(
            'flow.telemetry.messenger.middleware',
            TracingMiddleware::class,
        );

        $handler = new TestMessageHandler();
        $capturingMiddleware = new CapturingMiddleware();

        $bus = new MessageBus([
            $middleware,
            $capturingMiddleware,
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [$handler],
            ])),
        ]);

        $bus->dispatch(new TestMessage('test'));

        static::assertNotNull($capturingMiddleware->captured);
        $stamp = $capturingMiddleware->captured->last(TelemetryStamp::class);
        static::assertNull($stamp);
    }

    public function test_handles_missing_stamp_on_consume_gracefully(): void
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
                        'messenger' => [
                            'enabled' => true,
                            'context_propagation' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $handler = new TestMessageHandler();

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [$handler],
            ])),
        ]);

        $envelope = new Envelope(new TestMessage('test'), [new ReceivedStamp('async')]);

        $bus->dispatch($envelope);

        static::assertTrue($handler->handled);

        $processor = $this->symfonyContext()->getService(
            'flow.telemetry.tracer_provider.processor',
            MemorySpanProcessor::class,
        );
        $spans = $processor->endedSpans();

        static::assertCount(1, $spans);
        static::assertSame(SpanKind::CONSUMER, $spans[0]->kind());
    }

    public function test_middleware_not_registered_when_disabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'instrumentation' => [
                        'messenger' => [
                            'enabled' => false,
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertFalse($container->has('flow.telemetry.messenger.middleware'));
    }

    public function test_middleware_service_is_registered(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'instrumentation' => [
                        'messenger' => [
                            'enabled' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.messenger.middleware'));
        static::assertInstanceOf(TracingMiddleware::class, $container->get('flow.telemetry.messenger.middleware'));
    }

    public function test_producer_span_context_propagated_even_without_parent_context(): void
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
                        'messenger' => [
                            'enabled' => true,
                            'context_propagation' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $handler = new TestMessageHandler();
        $capturingMiddleware = new CapturingMiddleware();

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator),
            $capturingMiddleware,
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [$handler],
            ])),
        ]);

        $bus->dispatch(new TestMessage('test'));

        static::assertNotNull($capturingMiddleware->captured);
        $stamp = $capturingMiddleware->captured->last(TelemetryStamp::class);
        static::assertInstanceOf(TelemetryStamp::class, $stamp);

        $traceparent = $stamp->get('traceparent');
        static::assertNotNull($traceparent);

        $processor = $this->symfonyContext()->getService(
            'flow.telemetry.tracer_provider.processor',
            MemorySpanProcessor::class,
        );
        $spans = $processor->endedSpans();
        static::assertCount(1, $spans);

        $producerSpan = $spans[0];
        static::assertSame(SpanKind::PRODUCER, $producerSpan->kind());
        static::assertStringContainsString($producerSpan->context()->spanId->toHex(), $traceparent);
    }

    public function test_propagator_baggage_only_configuration(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'propagator' => ['type' => 'baggage'],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.propagator'));
        static::assertFalse($container->has('flow.telemetry.propagator.tracecontext'));
        static::assertFalse($container->has('flow.telemetry.propagator.baggage'));
    }

    public function test_propagator_tracecontext_only_configuration(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'propagator' => ['type' => 'tracecontext'],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.propagator'));
        static::assertFalse($container->has('flow.telemetry.propagator.tracecontext'));
        static::assertFalse($container->has('flow.telemetry.propagator.baggage'));
    }

    public function test_propagator_w3c_configuration(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'propagator' => ['type' => 'w3c'],
                ]);
            },
        ]);

        $container = $this->getContainer();

        static::assertTrue($container->has('flow.telemetry.propagator'));
        static::assertTrue($container->has('flow.telemetry.propagator.tracecontext'));
        static::assertTrue($container->has('flow.telemetry.propagator.baggage'));
    }

    public function test_traces_message_dispatch(): void
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
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var Telemetry $telemetry */
        $telemetry = $container->get(Telemetry::class);

        $handler = new TestMessageHandler();

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [$handler],
            ])),
        ]);

        $message = new TestMessage('test content');
        $envelope = new Envelope($message, [new BusNameStamp('command.bus')]);

        $bus->dispatch($envelope);

        static::assertTrue($handler->handled);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(1, $spans);

        $span = $spans[0];
        static::assertSame('command.bus TestMessage', $span->name());
        static::assertSame(SpanKind::PRODUCER, $span->kind());

        $attributes = $span->attributes();
        static::assertSame('symfony_messenger', $attributes['messaging.system']);
        static::assertSame('command.bus', $attributes['messaging.destination.name']);
        static::assertSame(TestMessage::class, $attributes['messaging.message.class']);
        static::assertSame('send', $attributes['messaging.operation.type']);
        static::assertSame(TestMessage::class, $attributes['messaging.operation.name']);

        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isOk());
    }

    public function test_traces_message_with_exception(): void
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
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        /** @var Telemetry $telemetry */
        $telemetry = $container->get(Telemetry::class);

        $failingHandler = static function (TestMessage $message): void {
            throw new RuntimeException('Handler failed');
        };

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [$failingHandler],
            ])),
        ]);

        $message = new TestMessage('test content');

        $exceptionThrown = false;

        try {
            $bus->dispatch($message);
        } catch (Throwable) {
            $exceptionThrown = true;
        }

        static::assertTrue($exceptionThrown, 'Expected exception was not thrown');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        static::assertCount(1, $spans);

        $span = $spans[0];

        $status = $span->status();
        static::assertNotNull($status);
        static::assertTrue($status->isError());
        static::assertStringContainsString('Handler failed', $status->description ?? '');

        $events = $span->events();
        static::assertCount(1, $events);
        static::assertSame('exception', $events[0]->name());
    }
}
