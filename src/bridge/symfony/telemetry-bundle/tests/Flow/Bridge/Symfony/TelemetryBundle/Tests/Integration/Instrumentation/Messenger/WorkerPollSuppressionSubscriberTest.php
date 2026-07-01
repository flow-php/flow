<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Messenger;

use ArrayIterator;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\MessengerHandlerLink;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\WorkerPollSuppressionSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\MessageHandler\TestMessageHandler;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Propagation\Propagator;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\HttpKernel\DependencyInjection\ServicesResetter;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\Event\WorkerStoppedEvent;
use Symfony\Component\Messenger\EventListener\ResetServicesListener;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Worker;

use function interface_exists;

#[CoversClass(WorkerPollSuppressionSubscriber::class)]
final class WorkerPollSuppressionSubscriberTest extends KernelTestCase
{
    protected function setUp(): void
    {
        if (!interface_exists(MiddlewareInterface::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }

        parent::setUp();
    }

    public function test_poll_spans_are_suppressed_while_the_handler_span_is_still_recorded(): void
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
                        'messenger' => ['enabled' => true, 'trace' => 'handlers', 'link' => 'dispatcher'],
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

        $resetter = new ServicesResetter(new ArrayIterator(['cs' => $contextStorage]), ['cs' => 'reset']);
        $dispatcher = new EventDispatcher();
        $dispatcher->addSubscriber(new ResetServicesListener($resetter));
        $dispatcher->addSubscriber(new WorkerPollSuppressionSubscriber($contextStorage));
        $worker = new Worker([], $bus, $dispatcher);

        $dbalTracer = $telemetry->tracer('flow.symfony.dbal', 'test');

        $dispatcher->dispatch(new WorkerStartedEvent($worker));

        // Transport claim query during the poll — must be suppressed (no orphan).
        $dbalTracer->complete($dbalTracer->span('doctrine.dbal.transaction.begin'));

        $bus->dispatch(new Envelope(new TestMessage('a'), [new ReceivedStamp('async')]));

        // Transport ack query after handling — suppressed again.
        $dbalTracer->complete($dbalTracer->span('doctrine.dbal.connection.exec'));

        $dispatcher->dispatch(new WorkerStoppedEvent($worker));

        $processor = $this->symfonyContext()->getService(
            'flow.telemetry.tracer_provider.processor',
            MemorySpanProcessor::class,
        );

        $names = [];

        foreach ($processor->endedSpans() as $span) {
            $names[] = $span->name();
        }

        static::assertContains('process TestMessage', $names, 'the handler span is recorded');
        static::assertNotContains('doctrine.dbal.transaction.begin', $names, 'the poll claim query is suppressed');
        static::assertNotContains('doctrine.dbal.connection.exec', $names, 'the ack query is suppressed');
    }
}
