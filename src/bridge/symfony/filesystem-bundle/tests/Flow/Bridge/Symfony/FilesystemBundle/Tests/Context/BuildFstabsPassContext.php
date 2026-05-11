<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Context;

use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\MemoryFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\NativeLocalFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactoryRegistry;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;

final class BuildFstabsPassContext
{
    /**
     * @param array<string, mixed> $config
     */
    public function containerWithConfig(array $config): ContainerBuilder
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.filesystem.config', $config);

        $registry = new Definition(FilesystemFactoryRegistry::class);
        $registry->setArgument(0, []);
        $container->setDefinition('.flow.filesystem.factory_registry', $registry);

        $memoryFactory = new Definition(MemoryFilesystemFactory::class);
        $memoryFactory->addTag('flow.filesystem.factory', ['type' => 'memory']);
        $container->setDefinition('.flow.filesystem.factory.memory', $memoryFactory);

        $nativeFactory = new Definition(NativeLocalFilesystemFactory::class);
        $nativeFactory->addTag('flow.filesystem.factory', ['type' => 'file']);
        $container->setDefinition('.flow.filesystem.factory.file', $nativeFactory);

        return $container;
    }
}
