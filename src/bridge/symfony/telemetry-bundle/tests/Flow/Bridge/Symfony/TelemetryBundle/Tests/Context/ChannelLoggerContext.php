<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Context;

use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Channel\FrameworkChannelConsumer;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

final readonly class ChannelLoggerContext
{
    public function frameworkConsumer(string $channel): Definition
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
    public function publicDefinition(string $class): Definition
    {
        $definition = new Definition($class);
        $definition->setPublic(true);

        return $definition;
    }

    /**
     * @param class-string $class
     */
    public function autoconfiguredConsumer(string $class): Definition
    {
        $definition = new Definition($class);
        $definition->setAutowired(true);
        $definition->setAutoconfigured(true);
        $definition->setPublic(true);

        return $definition;
    }
}
