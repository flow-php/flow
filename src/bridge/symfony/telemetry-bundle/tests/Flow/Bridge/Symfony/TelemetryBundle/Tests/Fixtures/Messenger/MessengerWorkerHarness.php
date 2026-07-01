<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Messenger;

use ArrayIterator;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use Psr\Container\ContainerInterface;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
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

final class MessengerWorkerHarness
{
    /**
     * @return array<string>
     */
    public static function drive(ContainerInterface $container, EventSubscriberInterface $workerSubscriber): array
    {
        /** @var Telemetry $telemetry */
        $telemetry = $container->get(Telemetry::class);
        /** @var ContextStorage $contextStorage */
        $contextStorage = $container->get('flow.telemetry.context_storage');
        /** @var MiddlewareInterface $middleware */
        $middleware = $container->get('flow.telemetry.messenger.middleware');

        $bus = new MessageBus([
            $middleware,
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [static function (TestMessage $message) use ($telemetry): void {
                    $tracer = $telemetry->tracer('app', 'test');
                    $tracer->complete($tracer->span('handler.work'));
                }],
            ])),
        ]);

        $resetter = new ServicesResetter(new ArrayIterator(['cs' => $contextStorage]), ['cs' => 'reset']);
        $dispatcher = new EventDispatcher();
        $dispatcher->addSubscriber(new ResetServicesListener($resetter));
        $dispatcher->addSubscriber($workerSubscriber);
        $worker = new Worker([], $bus, $dispatcher);

        $pollTracer = $telemetry->tracer('flow.symfony.dbal', 'test');

        $dispatcher->dispatch(new WorkerStartedEvent($worker));
        $pollTracer->complete($pollTracer->span('poll.query'));
        $bus->dispatch(new Envelope(new TestMessage('a'), [new ReceivedStamp('async')]));
        $dispatcher->dispatch(new WorkerStoppedEvent($worker));

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');

        $names = [];

        foreach ($processor->endedSpans() as $span) {
            $names[] = $span->name();
        }

        return $names;
    }
}
