<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Context;

use Flow\Bridge\Symfony\PostgreSqlBundle\FlowPostgreSqlBundle;
use LogicException;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Extension\ExtensionInterface;

final readonly class ExtensionContext
{
    /**
     * @param array<string, mixed> $config
     */
    public function load(array $config): ContainerBuilder
    {
        $extension = (new FlowPostgreSqlBundle())->getContainerExtension();

        if (!$extension instanceof ExtensionInterface) {
            throw new LogicException('FlowPostgreSqlBundle extension is not loadable.');
        }

        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.debug', false);
        $container->setParameter('kernel.build_dir', '/tmp');
        $container->setParameter('kernel.project_dir', '/tmp');
        $extension->load([$config], $container);

        return $container;
    }
}
