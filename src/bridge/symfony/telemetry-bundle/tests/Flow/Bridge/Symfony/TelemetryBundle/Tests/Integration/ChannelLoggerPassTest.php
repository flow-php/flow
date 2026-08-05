<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration;

use Flow\Bridge\Symfony\TelemetryBundle\Attribute\WithTelemetryChannel;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ChannelLoggerPass;
use Flow\Bridge\Symfony\TelemetryBundle\FlowTelemetryBundle;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Context\ChannelLoggerContext;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel\AppChannelConsumer;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel\EventsChannelConsumer;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel\FrameworkChannelConsumer;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel\NamedArgumentConsumer;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel\NativeChannelConsumer;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Flow\Telemetry\Logger\Logger;
use PHPUnit\Framework\Attributes\CoversClass;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Log\Logger as SymfonyDefaultLogger;

#[CoversClass(FlowTelemetryBundle::class)]
#[CoversClass(ChannelLoggerPass::class)]
#[CoversClass(WithTelemetryChannel::class)]
final class ChannelLoggerPassTest extends KernelTestCase
{
    public function test_app_channel_attribute_binds_its_own_channel_logger(): void
    {
        $channelContext = new ChannelLoggerContext();

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) use ($channelContext): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $channelContext,
                ): void {
                    $container->setDefinition(
                        'test.app_consumer',
                        $channelContext->autoconfiguredConsumer(AppChannelConsumer::class),
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
        $channelContext = new ChannelLoggerContext();

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) use ($channelContext): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $channelContext,
                ): void {
                    $container->setDefinition(
                        'test.events_consumer',
                        $channelContext->autoconfiguredConsumer(EventsChannelConsumer::class),
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
        $channelContext = new ChannelLoggerContext();

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) use ($channelContext): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $channelContext,
                ): void {
                    $container->setDefinition(
                        'test.native_consumer',
                        $channelContext->autoconfiguredConsumer(NativeChannelConsumer::class),
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
        $channelContext = new ChannelLoggerContext();

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) use ($channelContext): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $channelContext,
                ): void {
                    $container->setDefinition(
                        'test.events_consumer',
                        $channelContext->autoconfiguredConsumer(EventsChannelConsumer::class),
                    );
                });
            },
        ]);

        $logger = $this->getContainer()->get('flow.telemetry.events.logger');

        static::assertInstanceOf(Logger::class, $logger);
        static::assertSame('events', $logger->instrumentationScope()->attributes->get('flow.log.channel'));
    }

    public function test_named_argument_alias_resolves_to_the_channel_logger(): void
    {
        $channelContext = new ChannelLoggerContext();

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) use ($channelContext): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['resource' => []]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $channelContext,
                ): void {
                    $container->setDefinition(
                        'test.events_consumer',
                        $channelContext->autoconfiguredConsumer(EventsChannelConsumer::class),
                    );
                    $container->setDefinition(
                        'test.named_arg_consumer',
                        $channelContext->autoconfiguredConsumer(NamedArgumentConsumer::class),
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
        $channelContext = new ChannelLoggerContext();

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) use ($channelContext): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'capture_framework_channels' => true,
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $channelContext,
                ): void {
                    $container->setDefinition('logger', $channelContext->publicDefinition(SymfonyDefaultLogger::class));
                    $container->setDefinition('test.framework_consumer', $channelContext->frameworkConsumer('events'));
                });
            },
        ]);

        $consumer = $this->getContainer()->get('test.framework_consumer');

        static::assertInstanceOf(FrameworkChannelConsumer::class, $consumer);
        static::assertSame($this->getContainer()->get('flow.telemetry.events.logger.psr3'), $consumer->logger);
    }

    public function test_does_not_capture_framework_channels_when_disabled(): void
    {
        $channelContext = new ChannelLoggerContext();

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) use ($channelContext): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'resource' => [],
                    'capture_framework_channels' => false,
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $channelContext,
                ): void {
                    $container->setDefinition('logger', $channelContext->publicDefinition(SymfonyDefaultLogger::class));
                    $container->setDefinition('test.framework_consumer', $channelContext->frameworkConsumer('events'));
                });
            },
        ]);

        $consumer = $this->getContainer()->get('test.framework_consumer');

        static::assertInstanceOf(FrameworkChannelConsumer::class, $consumer);
        static::assertSame($this->getContainer()->get('flow.telemetry.default.logger.psr3'), $consumer->logger);
        static::assertFalse($this->getContainer()->has('flow.telemetry.events.logger.psr3'));
    }

    public function test_channel_attribute_still_autowires_when_telemetry_is_disabled(): void
    {
        $channelContext = new ChannelLoggerContext();

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) use ($channelContext): void {
                $kernel->addTestExtensionConfig('flow_telemetry', ['enabled' => false, 'resource' => []]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $channelContext,
                ): void {
                    $container->setDefinition(
                        'test.app_consumer',
                        $channelContext->autoconfiguredConsumer(AppChannelConsumer::class),
                    );
                    $container->setDefinition(
                        'test.native_consumer',
                        $channelContext->autoconfiguredConsumer(NativeChannelConsumer::class),
                    );
                });
            },
        ]);

        $psrConsumer = $this->getContainer()->get('test.app_consumer');
        $nativeConsumer = $this->getContainer()->get('test.native_consumer');

        static::assertInstanceOf(AppChannelConsumer::class, $psrConsumer);
        static::assertSame($this->getContainer()->get('flow.telemetry.app.logger.psr3'), $psrConsumer->logger);

        static::assertInstanceOf(NativeChannelConsumer::class, $nativeConsumer);
        static::assertSame($this->getContainer()->get('flow.telemetry.events.logger'), $nativeConsumer->logger);
    }

    public function test_framework_channels_are_never_captured_when_telemetry_is_disabled(): void
    {
        $channelContext = new ChannelLoggerContext();

        $this->bootKernel([
            'config' => static function (TestKernel $kernel) use ($channelContext): void {
                $kernel->addTestExtensionConfig('flow_telemetry', [
                    'enabled' => false,
                    'resource' => [],
                    'capture_framework_channels' => true,
                ]);
                $kernel->addTestContainerConfigurator(static function (ContainerBuilder $container) use (
                    $channelContext,
                ): void {
                    $container->setDefinition('logger', $channelContext->publicDefinition(SymfonyDefaultLogger::class));
                    $container->setDefinition('test.framework_consumer', $channelContext->frameworkConsumer('events'));
                });
            },
        ]);

        $consumer = $this->getContainer()->get('test.framework_consumer');

        static::assertInstanceOf(FrameworkChannelConsumer::class, $consumer);
        static::assertInstanceOf(SymfonyDefaultLogger::class, $consumer->logger);
        static::assertFalse($this->getContainer()->has('flow.telemetry.events.logger.psr3'));
    }
}
