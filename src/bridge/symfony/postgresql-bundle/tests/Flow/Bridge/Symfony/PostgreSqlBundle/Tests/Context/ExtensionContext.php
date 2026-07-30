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
     * @param list<class-string> $bundles registered kernel bundles, e.g. WebProfilerBundle
     */
    public function load(array $config, bool $debug = false, array $bundles = []): ContainerBuilder
    {
        $extension = (new FlowPostgreSqlBundle())->getContainerExtension();

        if (!$extension instanceof ExtensionInterface) {
            throw new LogicException('FlowPostgreSqlBundle extension is not loadable.');
        }

        $container = new ContainerBuilder();
        $container->setParameter('kernel.environment', 'test');
        $container->setParameter('kernel.debug', $debug);
        $container->setParameter('kernel.build_dir', '/tmp');
        $container->setParameter('kernel.project_dir', '/tmp');
        $container->setParameter('kernel.bundles', $bundles);
        $extension->load([$config], $container);

        return $container;
    }
}
