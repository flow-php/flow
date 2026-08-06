<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ChannelLoggerPass;
use Psr\Log\LoggerInterface;
use Symfony\Component\DependencyInjection\Argument\BoundArgument;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

use function Flow\Types\DSL\type_instance_of;

final class ChannelLoggerMother
{
    public static function frameworkConsumerContainer(string $channel): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.capture_framework_channels', true);
        $consumer = new Definition();
        $consumer->addTag('monolog.logger', ['channel' => $channel]);
        $container->setDefinition('framework.consumer', $consumer);

        return $container;
    }

    public static function taggedConsumerContainer(string $channel): ContainerBuilder
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
    public static function loggerBinding(Definition $definition, string $type = LoggerInterface::class): Reference
    {
        $binding = type_instance_of(BoundArgument::class)->assert($definition->getBindings()[$type] ?? null);

        return type_instance_of(Reference::class)->assert($binding->getValues()[0] ?? null);
    }
}
