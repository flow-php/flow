<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Context;

use Flow\Bridge\Symfony\TelemetryBundle\FlowTelemetryBundle;
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
        $extension = (new FlowTelemetryBundle())->getContainerExtension();

        if (!$extension instanceof ConfigurationExtensionInterface) {
            throw new LogicException('FlowTelemetryBundle extension does not expose a configuration tree.');
        }

        $configuration = $extension->getConfiguration([], new ContainerBuilder());

        if ($configuration === null) {
            throw new LogicException('FlowTelemetryBundle extension exposes no configuration tree.');
        }

        return (new Processor())->processConfiguration($configuration, [$config]);
    }
}
