<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\ConsumeMessagesCommandMother;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use Psr\Container\ContainerInterface;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\Event\WorkerStoppedEvent;
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

        $dispatcher = new EventDispatcher();
        $dispatcher->addSubscriber($workerSubscriber);
        $worker = new Worker([], $bus, $dispatcher);

        $pollTracer = $telemetry->tracer('flow.symfony.dbal', 'test');

        $command = ConsumeMessagesCommandMother::create();

        $dispatcher->dispatch(
            new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput()),
            ConsoleEvents::COMMAND,
        );
        $dispatcher->dispatch(new WorkerStartedEvent($worker));
        $pollTracer->complete($pollTracer->span('poll.query'));
        $bus->dispatch(new Envelope(new TestMessage('a'), [new ReceivedStamp('async')]));
        $dispatcher->dispatch(new WorkerStoppedEvent($worker));
        $dispatcher->dispatch(
            new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0),
            ConsoleEvents::TERMINATE,
        );

        /** @var MemorySpanProcessor $processor */
        $processor = $container->get('flow.telemetry.tracer_provider.processor');

        $names = [];

        foreach ($processor->endedSpans() as $span) {
            $names[] = $span->name();
        }

        return $names;
    }
}
