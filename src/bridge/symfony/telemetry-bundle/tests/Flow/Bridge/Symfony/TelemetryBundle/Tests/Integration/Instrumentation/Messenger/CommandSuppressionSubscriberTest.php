<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\CommandSuppressionSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\ConsoleSpanSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\MessageHandler\TestMessageHandler;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\ConsumeMessagesCommandMother;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Propagation\Propagator;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Flow\Telemetry\Telemetry;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\NullOutput;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Stamp\ConsumedByWorkerStamp;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Worker;

use function interface_exists;

#[CoversClass(CommandSuppressionSubscriber::class)]
final class CommandSuppressionSubscriberTest extends KernelTestCase
{
    protected function setUp(): void
    {
        if (!interface_exists(MiddlewareInterface::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }

        parent::setUp();
    }

    public function test_worker_loop_is_suppressed_while_handler_traces_are_recorded(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => ['type' => 'memory', 'exporter' => 'memory'],
                    ],
                    'propagator' => ['type' => 'w3c'],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => false,
                        'messenger' => ['enabled' => true, 'trace' => true],
                    ],
                ]);
            },
        ]);

        $this->getContainer();

        $telemetry = $this->symfonyContext()->getService(Telemetry::class, Telemetry::class);
        $contextStorage = $this->symfonyContext()->getService('flow.telemetry.context_storage', ContextStorage::class);
        $propagator = $this->symfonyContext()->getService('flow.telemetry.propagator', Propagator::class);

        $bus = new MessageBus([
            new TracingMiddleware($telemetry, $contextStorage, $propagator, true),
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [new TestMessageHandler()],
            ])),
        ]);

        $dispatcher = new EventDispatcher();
        $dispatcher->addSubscriber(new ConsoleSpanSubscriber($telemetry));
        $dispatcher->addSubscriber(new CommandSuppressionSubscriber($contextStorage, ['messenger:consume']));

        $dbalTracer = $telemetry->tracer('flow.symfony.dbal', 'test');

        // Zenstruck-style co-listener doing I/O on every tick at default priority 0 — the original leak.
        $dispatcher->addListener(
            WorkerRunningEvent::class,
            static function () use ($dbalTracer): void {
                $dbalTracer->complete($dbalTracer->span('doctrine.dbal.tick'));
            },
            0,
        );

        $worker = new Worker([], $bus, $dispatcher);
        $command = ConsumeMessagesCommandMother::create();

        $dispatcher->dispatch(
            new ConsoleCommandEvent($command, new ArrayInput([]), new NullOutput()),
            ConsoleEvents::COMMAND,
        );

        // Bootstrap window (before WorkerStarted) — must be suppressed.
        $dbalTracer->complete($dbalTracer->span('doctrine.dbal.bootstrap'));

        $dispatcher->dispatch(new WorkerStartedEvent($worker));

        // Transport poll — suppressed.
        $dbalTracer->complete($dbalTracer->span('doctrine.dbal.poll'));

        $bus->dispatch(new Envelope(new TestMessage('a'), [new ReceivedStamp('async'), new ConsumedByWorkerStamp()]));

        // Tick between messages: the co-listener leak attempt must be suppressed.
        $dispatcher->dispatch(new WorkerRunningEvent($worker, true));

        // Second message — handler tracing must still work.
        $bus->dispatch(new Envelope(new TestMessage('b'), [new ReceivedStamp('async'), new ConsumedByWorkerStamp()]));

        $dispatcher->dispatch(
            new ConsoleTerminateEvent($command, new ArrayInput([]), new NullOutput(), 0),
            ConsoleEvents::TERMINATE,
        );

        $processor = $this->symfonyContext()->getService(
            'flow.telemetry.tracer_provider.processor',
            MemorySpanProcessor::class,
        );

        $names = [];

        foreach ($processor->endedSpans() as $span) {
            $names[] = $span->name();
        }

        $handlerSpans = array_filter($names, static fn(string $n): bool => $n === 'process async');

        static::assertCount(2, $handlerSpans, 'both handler spans are recorded');
        static::assertNotContains('doctrine.dbal.bootstrap', $names, 'the bootstrap window is suppressed');
        static::assertNotContains('doctrine.dbal.poll', $names, 'the transport poll is suppressed');
        static::assertNotContains('doctrine.dbal.tick', $names, 'the per-tick co-listener span is suppressed');
        static::assertNotContains('messenger:consume', $names, 'the long-lived console span is dropped in the worker');
    }
}
