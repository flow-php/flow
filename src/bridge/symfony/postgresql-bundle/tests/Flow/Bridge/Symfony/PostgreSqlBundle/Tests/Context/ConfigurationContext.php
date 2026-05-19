<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Context;

use Flow\Bridge\Symfony\PostgreSqlBundle\FlowPostgreSqlBundle;
use LogicException;
use Symfony\Component\Config\Definition\Processor;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Extension\ConfigurationExtensionInterface;

final readonly class ConfigurationContext
{
    /**
     * @param array<string, mixed> $config
     *
     * @return array<mixed>
     */
    public function processConfig(array $config): array
    {
        $extension = (new FlowPostgreSqlBundle())->getContainerExtension();

        if (!$extension instanceof ConfigurationExtensionInterface) {
            throw new LogicException('FlowPostgreSqlBundle extension does not expose a configuration tree.');
        }

        $configuration = $extension->getConfiguration([], new ContainerBuilder());

        if ($configuration === null) {
            throw new LogicException('FlowPostgreSqlBundle extension exposes no configuration tree.');
        }

        return (new Processor())->processConfiguration($configuration, [$config]);
    }
}
