<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Context;

use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\ResolveFilesystemArgumentsPass;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;

final class ResolveFilesystemArgumentsPassContext
{
    /**
     * @param array<string, mixed> $config
     */
    public function containerWithConfig(array $config): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.filesystem.config', $config);

        return $container;
    }

    /**
     * @param array{argument: string, mount: string, fstab?: string} $tag
     */
    public function registerConsumer(ContainerBuilder $container, string $serviceId, array $tag): Definition
    {
        $definition = new Definition('Flow\\Consumer');
        $definition->addTag(ResolveFilesystemArgumentsPass::TAG, $tag);
        $container->setDefinition($serviceId, $definition);

        return $definition;
    }
}
