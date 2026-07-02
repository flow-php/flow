<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\Instrumentation\Messenger;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\MessengerTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message\TestMessage;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\MessageHandler\TestMessageHandler;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration\KernelTestCase;
use Flow\Telemetry\Provider\Memory\MemorySpanProcessor;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Bundle\FrameworkBundle\FrameworkBundle;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;

use function array_map;
use function interface_exists;
use function restore_exception_handler;

#[CoversClass(MessengerTelemetryPass::class)]
final class MessengerMiddlewareInjectionTest extends KernelTestCase
{
    protected function setUp(): void
    {
        if (!interface_exists(MiddlewareInterface::class)) {
            self::markTestSkipped('symfony/messenger is not installed');
        }

        parent::setUp();
    }

    #[Override]
    protected function tearDown(): void
    {
        restore_exception_handler();
        parent::tearDown();
    }

    public function test_tracing_middleware_is_injected_into_the_framework_message_bus(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestBundle(FrameworkBundle::class);
                $kernel->addTestExtensionConfig('framework', [
                    'http_method_override' => false,
                    'handle_all_throwables' => true,
                    'messenger' => [
                        'default_bus' => 'messenger.bus.default',
                        'buses' => ['messenger.bus.default' => null],
                    ],
                ]);
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'exporters' => ['memory' => ['memory' => null], 'void' => ['void' => null]],
                    'tracer_provider' => ['processor' => ['type' => 'memory', 'exporter' => 'memory']],
                    'instrumentation' => [
                        'http_kernel' => ['enabled' => false],
                        'console' => ['enabled' => false],
                        'messenger' => ['enabled' => true],
                    ],
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $handler = new Definition(TestMessageHandler::class);
                    $handler->addTag('messenger.message_handler', ['handles' => TestMessage::class]);
                    $container->setDefinition('test.message_handler', $handler);

                    $container->setAlias('test.message_bus', 'messenger.bus.default')->setPublic(true);
                });
            },
        ]);

        $bus = $this->getContainer()->get('test.message_bus');
        static::assertInstanceOf(MessageBusInterface::class, $bus);

        $bus->dispatch(new TestMessage('x'));

        $processor = $this->symfonyContext()->getService(
            'flow.telemetry.tracer_provider.processor',
            MemorySpanProcessor::class,
        );

        $names = array_map(static fn($span): string => $span->name(), $processor->endedSpans());

        static::assertContains(
            'send TestMessage',
            $names,
            'the tracing middleware was injected into the framework-configured bus and ran on dispatch',
        );
    }
}
