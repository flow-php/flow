<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ChannelLoggerPass;
use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Flow\Telemetry\Attributes;
use Flow\Telemetry\Logger\Logger;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Symfony\Component\DependencyInjection\Argument\BoundArgument;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

#[CoversClass(ChannelLoggerPass::class)]
final class ChannelLoggerPassTest extends TestCase
{
    public function test_app_channel_is_synthesized_like_any_other_channel(): void
    {
        $container = $this->containerWithTaggedConsumer('app');

        (new ChannelLoggerPass())->process($container);

        static::assertTrue($container->hasDefinition('flow.telemetry.app.logger'));
        static::assertSame(
            'flow.telemetry.app.logger.psr3',
            (string) $this->loggerBinding($container->getDefinition('consumer')),
        );
    }

    public function test_default_channel_reuses_the_always_present_default_logger(): void
    {
        $container = $this->containerWithTaggedConsumer('default');
        $existing = new Definition(Logger::class);
        $container->setDefinition('flow.telemetry.default.logger', $existing);
        $container->setDefinition('flow.telemetry.default.logger.psr3', new Definition(Logger::class));

        (new ChannelLoggerPass())->process($container);

        static::assertSame($existing, $container->getDefinition('flow.telemetry.default.logger'));
        static::assertSame(
            'flow.telemetry.default.logger.psr3',
            (string) $this->loggerBinding($container->getDefinition('consumer')),
        );
    }

    public function test_binds_channel_logger_to_tagged_service(): void
    {
        $container = $this->containerWithTaggedConsumer('events');

        (new ChannelLoggerPass())->process($container);

        static::assertSame(
            'flow.telemetry.events.logger.psr3',
            (string) $this->loggerBinding($container->getDefinition('consumer')),
        );
    }

    public function test_binds_native_logger_to_tagged_service(): void
    {
        $container = $this->containerWithTaggedConsumer('events');

        (new ChannelLoggerPass())->process($container);

        static::assertSame(
            'flow.telemetry.events.logger',
            (string) $this->loggerBinding($container->getDefinition('consumer'), Logger::class),
        );
    }

    public function test_app_channel_binds_its_own_native_logger(): void
    {
        $container = $this->containerWithTaggedConsumer('app');

        (new ChannelLoggerPass())->process($container);

        static::assertSame(
            'flow.telemetry.app.logger',
            (string) $this->loggerBinding($container->getDefinition('consumer'), Logger::class),
        );
    }

    public function test_registers_native_logger_named_argument_alias(): void
    {
        $container = $this->containerWithTaggedConsumer('events');

        (new ChannelLoggerPass())->process($container);

        $aliasId = Logger::class . ' $eventsLogger';
        static::assertTrue($container->hasAlias($aliasId));
        static::assertSame('flow.telemetry.events.logger', (string) $container->getAlias($aliasId));
    }

    public function test_does_not_override_a_declared_logger(): void
    {
        $container = $this->containerWithTaggedConsumer('events');
        $declared = new Definition(Logger::class);
        $container->setDefinition('flow.telemetry.events.logger', $declared);
        $container->setDefinition('flow.telemetry.events.logger.psr3', new Definition(Logger::class));

        (new ChannelLoggerPass())->process($container);

        static::assertSame($declared, $container->getDefinition('flow.telemetry.events.logger'));
    }

    public function test_empty_channel_attribute_throws(): void
    {
        $container = $this->containerWithTaggedConsumer('');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('missing the required non-empty "channel" attribute');

        (new ChannelLoggerPass())->process($container);
    }

    public function test_channel_parameter_resolving_to_empty_throws(): void
    {
        $container = $this->containerWithTaggedConsumer('%flow.test.channel%');
        $container->setParameter('flow.test.channel', '');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('did not resolve to a non-empty string');

        (new ChannelLoggerPass())->process($container);
    }

    public function test_missing_channel_attribute_throws(): void
    {
        $container = new ContainerBuilder();
        $consumer = new Definition();
        $consumer->addTag(ChannelLoggerPass::TAG);
        $container->setDefinition('consumer', $consumer);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Service "consumer" is tagged');

        (new ChannelLoggerPass())->process($container);
    }

    public function test_registers_camel_cased_named_argument_alias(): void
    {
        $container = $this->containerWithTaggedConsumer('http_client');

        (new ChannelLoggerPass())->process($container);

        $aliasId = LoggerInterface::class . ' $httpClientLogger';
        static::assertTrue($container->hasAlias($aliasId));
        static::assertSame('flow.telemetry.http_client.logger.psr3', (string) $container->getAlias($aliasId));
    }

    public function test_rewrites_explicit_logger_reference_in_arguments(): void
    {
        $container = $this->containerWithTaggedConsumer('events');
        $container->getDefinition('consumer')->setArgument(0, new Reference('logger'));

        (new ChannelLoggerPass())->process($container);

        static::assertSame(
            'flow.telemetry.events.logger.psr3',
            (string) $container->getDefinition('consumer')->getArgument(0),
        );
    }

    public function test_synthesizes_channel_logger_with_log_channel_on_both_scope_and_signal_by_default(): void
    {
        $container = $this->containerWithTaggedConsumer('events');

        (new ChannelLoggerPass())->process($container);

        $logger = $container->getDefinition('flow.telemetry.events.logger');

        // @mago-expect analysis:mixed-assignment
        $scope = $logger->getArgument(3);
        static::assertInstanceOf(Definition::class, $scope);
        static::assertSame(Attributes::class, $scope->getClass());
        static::assertSame(['log.channel' => 'events'], $scope->getArgument(0));

        // @mago-expect analysis:mixed-assignment
        $signal = $logger->getArgument(4);
        static::assertInstanceOf(Definition::class, $signal);
        static::assertSame(Attributes::class, $signal->getClass());
        static::assertSame(['log.channel' => 'events'], $signal->getArgument(0));
    }

    public function test_channel_attribute_target_scope_places_log_channel_on_scope_only(): void
    {
        $container = $this->containerWithTaggedConsumer('events');
        $container->setParameter('flow.telemetry.channel_attribute_target', 'scope');

        (new ChannelLoggerPass())->process($container);

        $logger = $container->getDefinition('flow.telemetry.events.logger');

        // @mago-expect analysis:mixed-assignment
        $scope = $logger->getArgument(3);
        static::assertInstanceOf(Definition::class, $scope);
        static::assertSame(['log.channel' => 'events'], $scope->getArgument(0));
        static::assertNull($logger->getArgument(4));
    }

    public function test_channel_attribute_target_signal_places_log_channel_on_signal_only(): void
    {
        $container = $this->containerWithTaggedConsumer('events');
        $container->setParameter('flow.telemetry.channel_attribute_target', 'signal');

        (new ChannelLoggerPass())->process($container);

        $logger = $container->getDefinition('flow.telemetry.events.logger');

        static::assertNull($logger->getArgument(3));
        // @mago-expect analysis:mixed-assignment
        $signal = $logger->getArgument(4);
        static::assertInstanceOf(Definition::class, $signal);
        static::assertSame(['log.channel' => 'events'], $signal->getArgument(0));
    }

    public function test_captures_framework_monolog_logger_channel(): void
    {
        $container = $this->containerWithFrameworkConsumer('request');

        (new ChannelLoggerPass())->process($container);

        static::assertSame(
            'flow.telemetry.request.logger.psr3',
            (string) $this->loggerBinding($container->getDefinition('framework.consumer')),
        );
    }

    public function test_does_not_capture_framework_channels_when_disabled(): void
    {
        $container = $this->containerWithFrameworkConsumer('request');
        $container->setParameter('flow.telemetry.capture_framework_channels', false);
        $container->getDefinition('framework.consumer')->setArgument(0, new Reference('logger'));

        (new ChannelLoggerPass())->process($container);

        static::assertSame('logger', (string) $container->getDefinition('framework.consumer')->getArgument(0));
        static::assertArrayNotHasKey(
            LoggerInterface::class,
            $container->getDefinition('framework.consumer')->getBindings(),
        );
    }

    public function test_explicit_channel_takes_precedence_over_framework_channel(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.capture_framework_channels', true);
        $consumer = new Definition();
        $consumer->addTag('monolog.logger', ['channel' => 'request']);
        $consumer->addTag(ChannelLoggerPass::TAG, ['channel' => 'events']);
        $container->setDefinition('consumer', $consumer);

        (new ChannelLoggerPass())->process($container);

        static::assertSame(
            'flow.telemetry.events.logger.psr3',
            (string) $this->loggerBinding($container->getDefinition('consumer')),
        );
    }

    public function test_preserves_invalid_reference_behavior_when_rewriting(): void
    {
        $container = $this->containerWithFrameworkConsumer('router');
        $container->getDefinition('framework.consumer')->setArgument(
            0,
            new Reference('logger', ContainerInterface::IGNORE_ON_INVALID_REFERENCE),
        );

        (new ChannelLoggerPass())->process($container);

        // @mago-expect analysis:mixed-assignment
        $reference = $container->getDefinition('framework.consumer')->getArgument(0);
        static::assertInstanceOf(Reference::class, $reference);
        static::assertSame('flow.telemetry.router.logger.psr3', (string) $reference);
        static::assertSame(ContainerInterface::IGNORE_ON_INVALID_REFERENCE, $reference->getInvalidBehavior());
    }

    public function test_registers_named_argument_alias_for_framework_channel(): void
    {
        $container = $this->containerWithFrameworkConsumer('http_client');

        (new ChannelLoggerPass())->process($container);

        $aliasId = LoggerInterface::class . ' $httpClientLogger';
        static::assertTrue($container->hasAlias($aliasId));
        static::assertSame('flow.telemetry.http_client.logger.psr3', (string) $container->getAlias($aliasId));
    }

    public function test_rewrites_logger_reference_in_method_call(): void
    {
        $container = $this->containerWithFrameworkConsumer('cache');
        $container->getDefinition('framework.consumer')->addMethodCall('setLogger', [new Reference('logger')]);

        (new ChannelLoggerPass())->process($container);

        // @mago-expect analysis:mixed-assignment
        $call = $container->getDefinition('framework.consumer')->getMethodCalls()[0] ?? null;
        static::assertIsArray($call);
        // @mago-expect analysis:mixed-assignment
        $arguments = $call[1] ?? null;
        static::assertIsArray($arguments);
        // @mago-expect analysis:mixed-assignment
        $argument = $arguments[0] ?? null;
        static::assertInstanceOf(Reference::class, $argument);
        static::assertSame('flow.telemetry.cache.logger.psr3', (string) $argument);
    }

    public function test_skips_framework_tag_whose_channel_resolves_to_empty(): void
    {
        $container = $this->containerWithFrameworkConsumer('%flow.test.channel%');
        $container->setParameter('flow.test.channel', '');
        $container->getDefinition('framework.consumer')->setArgument(0, new Reference('logger'));

        (new ChannelLoggerPass())->process($container);

        static::assertSame('logger', (string) $container->getDefinition('framework.consumer')->getArgument(0));
    }

    public function test_skips_framework_tag_without_a_channel(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.capture_framework_channels', true);
        $consumer = new Definition();
        $consumer->addTag('monolog.logger');
        $consumer->setArgument(0, new Reference('logger'));
        $container->setDefinition('framework.consumer', $consumer);

        (new ChannelLoggerPass())->process($container);

        static::assertSame('logger', (string) $container->getDefinition('framework.consumer')->getArgument(0));
    }

    private function containerWithFrameworkConsumer(string $channel): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.capture_framework_channels', true);
        $consumer = new Definition();
        $consumer->addTag('monolog.logger', ['channel' => $channel]);
        $container->setDefinition('framework.consumer', $consumer);

        return $container;
    }

    private function containerWithTaggedConsumer(string $channel): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $consumer = new Definition();
        $consumer->addTag(ChannelLoggerPass::TAG, ['channel' => $channel]);
        $container->setDefinition('consumer', $consumer);

        return $container;
    }

    /**
     * @param class-string $type
     */
    private function loggerBinding(Definition $definition, string $type = LoggerInterface::class): Reference
    {
        $binding = $definition->getBindings()[$type];
        self::assertInstanceOf(BoundArgument::class, $binding);
        // @mago-expect analysis:mixed-assignment
        $value = $binding->getValues()[0];
        self::assertInstanceOf(Reference::class, $value);

        return $value;
    }
}
