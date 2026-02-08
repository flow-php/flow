<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\MessageHandler\TestMessageHandler;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\SpanKind;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\Messenger\{Envelope, MessageBus};
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\Middleware\{HandleMessageMiddleware, MiddlewareInterface};
use Symfony\Component\Messenger\Stamp\BusNameStamp;

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

    public function test_middleware_not_registered_when_disabled() : void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
                    'instrumentation' => [
                        'messenger' => false,
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
                    'resource' => ['service' => ['name' => 'test-app']],
                    'instrumentation' => [
                        'messenger' => true,
                    ],
                ]);
            },
        ]);

        $container = $this->getContainer();

        self::assertTrue($container->has('flow.telemetry.messenger.middleware'));
        self::assertInstanceOf(TracingMiddleware::class, $container->get('flow.telemetry.messenger.middleware'));
    }

    public function test_traces_message_dispatch() : void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
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
        self::assertSame('command.bus', $attributes['messaging.destination']);
        self::assertSame(TestMessage::class, $attributes['messaging.message.class']);
        self::assertSame('send', $attributes['messaging.operation']);

        $status = $span->status();
        self::assertNotNull($status);
        self::assertTrue($status->isOk());
    }

    public function test_traces_message_with_exception() : void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel) : void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => ['service' => ['name' => 'test-app']],
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
