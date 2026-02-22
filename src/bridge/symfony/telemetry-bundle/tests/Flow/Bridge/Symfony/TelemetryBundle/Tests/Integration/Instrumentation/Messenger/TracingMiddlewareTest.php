<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\{TelemetryStamp, TracingMiddleware};
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\MessageHandler\TestMessageHandler;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Middleware\CapturingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Context\{Context, ContextStorage, TraceId};
use Flow\Telemetry\Propagation\Propagator;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\Messenger\{Envelope, MessageBus};
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\Middleware\{HandleMessageMiddleware, MiddlewareInterface};
use Symfony\Component\Messenger\Stamp\{BusNameStamp, ReceivedStamp};

#[CoversClass(TracingMiddleware::class)]
final class TracingMiddlewareTest extends KernelTestCase
{
    protected function setUp() : void
    {
        if (!\interface_exists(MiddlewareInterface::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }

        parent::setUp();
    }

    public function test_context_is_extracted_on_consume() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
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

        $container = $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $originalTraceId = 'abcdef0123456789abcdef0123456789';
        $originalSpanId = '0123456789abcdef';
        $traceparent = "00-{$originalTraceId}-{$originalSpanId}-01";

        $telemetryStamp = new TelemetryStamp(['traceparent' => $traceparent]);

        $handler = new TestMessageHandler();

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator),
            new HandleMessageMiddleware(
                new HandlersLocator([
                    TestMessage::class => [$handler],
                ])
            ),
        ]);

        $envelope = new Envelope(
            new TestMessage('test'),
            [
                new ReceivedStamp('async'),
                $telemetryStamp,
            ]
        );

        $bus->dispatch($envelope);

        $processor = $this->symfonyContext()->getService('flow.telemetry.tracer_provider.processor', MemorySpanProcessor::class);
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);
        $span = $spans[0];

        self::assertSame(SpanKind::CONSUMER, $span->kind());
        self::assertSame($originalTraceId, $span->context()->traceId->toHex());
    }

    public function test_context_is_injected_on_dispatch_when_context_propagation_enabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
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

        $container = $this->getContainer();

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
            new HandleMessageMiddleware(
                new HandlersLocator([
                    TestMessage::class => [$handler],
                ])
            ),
        ]);

        $bus->dispatch(new TestMessage('test'));

        $tracer->complete($span);

        self::assertNotNull($capturingMiddleware->captured);
        $stamp = $capturingMiddleware->captured->last(TelemetryStamp::class);
        self::assertInstanceOf(TelemetryStamp::class, $stamp);

        $traceparent = $stamp->get('traceparent');
        self::assertNotNull($traceparent);
        self::assertStringContainsString($traceId->toHex(), $traceparent);
    }

    public function test_context_propagation_not_applied_when_disabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
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

        $middleware = $this->symfonyContext()->getService('flow.telemetry.messenger.middleware', TracingMiddleware::class);

        $handler = new TestMessageHandler();
        $capturingMiddleware = new CapturingMiddleware();

        $bus = new MessageBus([
            $middleware,
            $capturingMiddleware,
            new HandleMessageMiddleware(
                new HandlersLocator([
                    TestMessage::class => [$handler],
                ])
            ),
        ]);

        $bus->dispatch(new TestMessage('test'));

        self::assertNotNull($capturingMiddleware->captured);
        $stamp = $capturingMiddleware->captured->last(TelemetryStamp::class);
        self::assertNull($stamp);
    }

    public function test_handles_missing_stamp_on_consume_gracefully() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
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
            new HandleMessageMiddleware(
                new HandlersLocator([
                    TestMessage::class => [$handler],
                ])
            ),
        ]);

        $envelope = new Envelope(
            new TestMessage('test'),
            [new ReceivedStamp('async')]
        );

        $bus->dispatch($envelope);

        self::assertTrue($handler->handled);

        $processor = $this->symfonyContext()->getService('flow.telemetry.tracer_provider.processor', MemorySpanProcessor::class);
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);
        self::assertSame(SpanKind::CONSUMER, $spans[0]->kind());
    }

    public function test_middleware_not_registered_when_disabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'instrumentation' => [
                        'messenger' => [
                            'enabled' => false,
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertFalse($container->has('flow.telemetry.messenger.middleware'));
    }

    public function test_middleware_service_is_registered() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'instrumentation' => [
                        'messenger' => [
                            'enabled' => true,
                        ],
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.messenger.middleware'));
        self::assertInstanceOf(TracingMiddleware::class, $container->get('flow.telemetry.messenger.middleware'));
    }

    public function test_producer_span_context_propagated_even_without_parent_context() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
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
            new HandleMessageMiddleware(
                new HandlersLocator([
                    TestMessage::class => [$handler],
                ])
            ),
        ]);

        $bus->dispatch(new TestMessage('test'));

        self::assertNotNull($capturingMiddleware->captured);
        $stamp = $capturingMiddleware->captured->last(TelemetryStamp::class);
        self::assertInstanceOf(TelemetryStamp::class, $stamp);

        $traceparent = $stamp->get('traceparent');
        self::assertNotNull($traceparent);

        $processor = $this->symfonyContext()->getService('flow.telemetry.tracer_provider.processor', MemorySpanProcessor::class);
        $spans = $processor->endedSpans();
        self::assertCount(1, $spans);

        $producerSpan = $spans[0];
        self::assertSame(SpanKind::PRODUCER, $producerSpan->kind());
        self::assertStringContainsString($producerSpan->context()->spanId->toHex(), $traceparent);
    }

    public function test_propagator_baggage_only_configuration() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'propagator' => ['type' => 'baggage'],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.propagator'));
        self::assertFalse($container->has('flow.telemetry.propagator.tracecontext'));
        self::assertFalse($container->has('flow.telemetry.propagator.baggage'));
    }

    public function test_propagator_tracecontext_only_configuration() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'propagator' => ['type' => 'tracecontext'],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.propagator'));
        self::assertFalse($container->has('flow.telemetry.propagator.tracecontext'));
        self::assertFalse($container->has('flow.telemetry.propagator.baggage'));
    }

    public function test_propagator_w3c_configuration() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'propagator' => ['type' => 'w3c'],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.propagator'));
        self::assertTrue($container->has('flow.telemetry.propagator.tracecontext'));
        self::assertTrue($container->has('flow.telemetry.propagator.baggage'));
    }

    public function test_traces_message_dispatch() : void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
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
            new HandleMessageMiddleware(
                new HandlersLocator([
                    TestMessage::class => [$handler],
                ])
            ),
        ]);

        $message = new TestMessage('test content');
        $envelope = new Envelope(
            $message,
            [new BusNameStamp('command.bus')]
        );

        $bus->dispatch($envelope);

        self::assertTrue($handler->handled);

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];
        self::assertSame('command.bus TestMessage', $span->name());
        self::assertSame(SpanKind::PRODUCER, $span->kind());

        $attributes = $span->attributes();
        self::assertSame('symfony_messenger', $attributes['messaging.system']);
        self::assertSame('command.bus', $attributes['messaging.destination.name']);
        self::assertSame(TestMessage::class, $attributes['messaging.message.class']);
        self::assertSame('send', $attributes['messaging.operation.type']);
        self::assertSame(TestMessage::class, $attributes['messaging.operation.name']);

        $status = $span->status();
        self::assertNotNull($status);
        self::assertTrue($status->isOk());
    }

    public function test_traces_message_with_exception() : void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'memory',
                            'exporter' => ['type' => 'memory'],
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

        $failingHandler = static function (TestMessage $message) : void {
            throw new \RuntimeException('Handler failed');
        };

        $bus = new MessageBus([
            new TracingMiddleware($telemetry),
            new HandleMessageMiddleware(
                new HandlersLocator([
                    TestMessage::class => [$failingHandler],
                ])
            ),
        ]);

        $message = new TestMessage('test content');

        $exceptionThrown = false;

        try {
            $bus->dispatch($message);
        } catch (\Throwable) {
            $exceptionThrown = true;
        }

        self::assertTrue($exceptionThrown, 'Expected exception was not thrown');

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');
        $spans = $processor->endedSpans();

        self::assertCount(1, $spans);

        $span = $spans[0];

        $status = $span->status();
        self::assertNotNull($status);
        self::assertTrue($status->isError());
        self::assertStringContainsString('Handler failed', $status->description ?? '');

        $events = $span->events();
        self::assertCount(1, $events);
        self::assertSame('exception', $events[0]->name());
    }
}
