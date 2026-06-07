<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration;

use Flow\Bridge\Symfony\TelemetryBundle\Attribute\WithTelemetryChannel;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ChannelLoggerPass;
use Flow\Bridge\Symfony\TelemetryBundle\FlowTelemetryBundle;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel\AppChannelConsumer;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel\EventsChannelConsumer;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel\FrameworkChannelConsumer;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel\NamedArgumentConsumer;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel\NativeChannelConsumer;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Telemetry\Logger\Logger;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\HttpKernel\Log\Logger as SymfonyDefaultLogger;

#[CoversClass(FlowTelemetryBundle::class)]
#[CoversClass(ChannelLoggerPass::class)]
#[CoversClass(WithTelemetryChannel::class)]
final class ChannelLoggerPassTest extends KernelTestCase
{
    public function test_app_channel_attribute_binds_its_own_channel_logger(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setDefinition(
                        'test.app_consumer',
                        self::autoconfiguredConsumer(AppChannelConsumer::class),
                    );
                });
            },
        ]);

        $consumer = $this->getContainer()->get('test.app_consumer');

        static::assertInstanceOf(AppChannelConsumer::class, $consumer);
        static::assertSame($this->getContainer()->get('flow.telemetry.app.logger.psr3'), $consumer->logger);
    }

    public function test_channel_attribute_binds_the_channel_logger_to_an_autowired_consumer(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setDefinition(
                        'test.events_consumer',
                        self::autoconfiguredConsumer(EventsChannelConsumer::class),
                    );
                });
            },
        ]);

        $consumer = $this->getContainer()->get('test.events_consumer');

        static::assertInstanceOf(EventsChannelConsumer::class, $consumer);
        static::assertSame($this->getContainer()->get('flow.telemetry.events.logger.psr3'), $consumer->logger);
    }

    public function test_channel_attribute_binds_the_native_logger_to_a_concrete_typehint(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setDefinition(
                        'test.native_consumer',
                        self::autoconfiguredConsumer(NativeChannelConsumer::class),
                    );
                });
            },
        ]);

        $consumer = $this->getContainer()->get('test.native_consumer');

        static::assertInstanceOf(NativeChannelConsumer::class, $consumer);
        static::assertSame($this->getContainer()->get('flow.telemetry.events.logger'), $consumer->logger);
    }

    public function test_channel_logger_carries_the_log_channel_scope_attribute(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setDefinition(
                        'test.events_consumer',
                        self::autoconfiguredConsumer(EventsChannelConsumer::class),
                    );
                });
            },
        ]);

        $logger = $this->getContainer()->get('flow.telemetry.events.logger');

        static::assertInstanceOf(Logger::class, $logger);
        static::assertSame('events', $logger->instrumentationScope()->attributes->get('log.channel'));
    }

    public function test_named_argument_alias_resolves_to_the_channel_logger(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setDefinition(
                        'test.events_consumer',
                        self::autoconfiguredConsumer(EventsChannelConsumer::class),
                    );
                    $container->setDefinition(
                        'test.named_arg_consumer',
                        self::autoconfiguredConsumer(NamedArgumentConsumer::class),
                    );
                });
            },
        ]);

        $consumer = $this->getContainer()->get('test.named_arg_consumer');

        static::assertInstanceOf(NamedArgumentConsumer::class, $consumer);
        static::assertSame($this->getContainer()->get('flow.telemetry.events.logger.psr3'), $consumer->eventsLogger);
    }

    public function test_captures_a_framework_tagged_service_into_a_channel_logger(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'capture_framework_channels' => true,
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setDefinition('logger', self::publicDefinition(SymfonyDefaultLogger::class));
                    $container->setDefinition('test.framework_consumer', self::frameworkConsumer('events'));
                });
            },
        ]);

        $consumer = $this->getContainer()->get('test.framework_consumer');

        static::assertInstanceOf(FrameworkChannelConsumer::class, $consumer);
        static::assertSame($this->getContainer()->get('flow.telemetry.events.logger.psr3'), $consumer->logger);
    }

    public function test_does_not_capture_framework_channels_when_disabled(): void
    {
        $this->bootKernel([
            'config' => static function (TestKernel $kernel): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'capture_framework_channels' => false,
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container): void {
                    $container->setDefinition('logger', self::publicDefinition(SymfonyDefaultLogger::class));
                    $container->setDefinition('test.framework_consumer', self::frameworkConsumer('events'));
                });
            },
        ]);

        $consumer = $this->getContainer()->get('test.framework_consumer');

        static::assertInstanceOf(FrameworkChannelConsumer::class, $consumer);
        static::assertSame($this->getContainer()->get('flow.telemetry.default.logger.psr3'), $consumer->logger);
        static::assertFalse($this->getContainer()->has('flow.telemetry.events.logger.psr3'));
    }

    private static function frameworkConsumer(string $channel): Definition
    {
        $definition = new Definition(FrameworkChannelConsumer::class);
        $definition->setArgument(0, new Reference('logger'));
        $definition->addTag('monolog.logger', ['channel' => $channel]);
        $definition->setPublic(true);

        return $definition;
    }

    /**
     * @param class-string $class
     */
    private static function publicDefinition(string $class): Definition
    {
        $definition = new Definition($class);
        $definition->setPublic(true);

        return $definition;
    }

    /**
     * @param class-string $class
     */
    private static function autoconfiguredConsumer(string $class): Definition
    {
        $definition = new Definition($class);
        $definition->setAutowired(true);
        $definition->setAutoconfigured(true);
        $definition->setPublic(true);

        return $definition;
    }
}
