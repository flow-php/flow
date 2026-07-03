<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Console;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Console\ConsoleFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Command\TestCommand;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Telemetry\SpySpanProcessor;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother\ConsumeMessagesCommandMother;
use Flow\Telemetry\Provider\Memory\MemoryExporter;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\Console\ConsoleEvents;
use Symfony\Component\Console\Event\ConsoleCommandEvent;
use Symfony\Component\Console\Event\ConsoleTerminateEvent;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Output\NullOutput;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerMessageHandledEvent;
use Symfony\Component\Messenger\Event\WorkerStartedEvent;
use Symfony\Component\Messenger\Event\WorkerStoppedEvent;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Stamp\ReceivedStamp;
use Symfony\Component\Messenger\Worker;

use function interface_exists;

#[CoversClass(ConsoleFlushSubscriber::class)]
final class ConsoleFlushSubscriberTest extends KernelTestCase
{
    #[Override]
    protected function tearDown(): void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_flush_is_called_on_terminate(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => [
                        'utf8' => true,
                        'resource' => __DIR__ . '/../../../Fixtures/config/routes.php',
                    ],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'batch_size' => 100,
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => true],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $application = new Application($kernel);
        $this->addCommand($application, new TestCommand());
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $input = new ArrayInput(['command' => 'test:command']);
        $output = new BufferedOutput();

        $exitCode = $application->run($input, $output);

        static::assertSame(0, $exitCode);

        $container = $this->getContainer();
        /** @var MemoryExporter $exporter */
        $exporter = $container->get('flow.telemetry.exporter.memory');
        $spans = $exporter->spans();

        static::assertCount(1, $spans, 'Spans should be exported after console terminate when flush is called');
    }

    public function test_flush_is_not_called_when_console_instrumentation_is_disabled(): void
    {
        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => [
                        'utf8' => true,
                        'resource' => __DIR__ . '/../../../Fixtures/config/routes.php',
                    ],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => [
                            'type' => 'batching',
                            'batch_size' => 100,
                            'exporter' => 'memory',
                        ],
                    ],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => false],
                        'messenger' => false,
                    ],
                ]);
            },
        ]);

        $application = new Application($kernel);
        $this->addCommand($application, new TestCommand());
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        $input = new ArrayInput(['command' => 'test:command']);
        $output = new BufferedOutput();

        $application->run($input, $output);

        $container = $this->getContainer();
        /** @var MemoryExporter $exporter */
        $exporter = $container->get('flow.telemetry.exporter.memory');
        $spans = $exporter->spans();

        static::assertCount(0, $spans, 'No spans should be exported when instrumentation is disabled');
    }

    public function test_nested_console_command_during_messenger_worker_does_not_shutdown_telemetry(): void
    {
        if (!interface_exists(MiddlewareInterface::class)) {
            static::markTestSkipped('symfony/messenger is not installed');
        }

        $kernel = $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'router' => [
                        'utf8' => true,
                        'resource' => __DIR__ . '/../../../Fixtures/config/routes.php',
                    ],
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => [
                        'processor' => ['type' => 'service', 'service_id' => 'app.spy_span_processor'],
                    ],
                    'instrumentation' => [
                        'http_kernel' => false,
                        'console' => ['enabled' => true],
                        'messenger' => ['enabled' => true, 'trace' => true],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $definition = new Definition(SpySpanProcessor::class);
                    $definition->setPublic(true);
                    $container->setDefinition('app.spy_span_processor', $definition);
                });
            },
        ]);

        $container = $this->getContainer();

        /** @var SpySpanProcessor $processor */
        $processor = $container->get('app.spy_span_processor');
        /** @var EventDispatcherInterface $dispatcher */
        $dispatcher = $container->get('event_dispatcher');

        $application = new Application($kernel);
        $this->addCommand($application, new TestCommand());
        $application->setAutoExit(false);
        $application->setCatchExceptions(false);

        /** @var MiddlewareInterface $middleware */
        $middleware = $container->get('flow.telemetry.messenger.middleware');

        $bus = new MessageBus([
            $middleware,
            new HandleMessageMiddleware(new HandlersLocator([
                TestMessage::class => [static function (TestMessage $message) use ($application): void {
                    $application->run(new ArrayInput(['command' => 'test:command']), new NullOutput());
                }],
            ])),
        ]);

        $worker = new Worker([], $bus, $dispatcher);
        $consumeCommand = ConsumeMessagesCommandMother::create();

        $dispatcher->dispatch(
            new ConsoleCommandEvent($consumeCommand, new ArrayInput([]), new NullOutput()),
            ConsoleEvents::COMMAND,
        );
        $dispatcher->dispatch(new WorkerStartedEvent($worker));

        $envelope = $bus->dispatch(new Envelope(new TestMessage('a'), [new ReceivedStamp('async')]));
        $dispatcher->dispatch(new WorkerMessageHandledEvent($envelope, 'async'));

        static::assertSame(
            0,
            $processor->shutdownCount,
            'Telemetry must not be shut down while the messenger worker is still consuming',
        );

        $dispatcher->dispatch(new WorkerStoppedEvent($worker));
        $dispatcher->dispatch(
            new ConsoleTerminateEvent($consumeCommand, new ArrayInput([]), new NullOutput(), 0),
            ConsoleEvents::TERMINATE,
        );

        static::assertSame(
            0,
            $processor->shutdownCount,
            'Terminate is only a flush point; shutdown happens at process end',
        );
        static::assertGreaterThan(0, $processor->flushCount, 'Telemetry is flushed on terminate');
    }
}
