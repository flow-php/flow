<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\DependencyInjection\Compiler;

use Symfony\Component\DependencyInjection\Argument\ServiceClosureArgument;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;
use Symfony\Component\DependencyInjection\ServiceLocator;

final class CommandLocatorPass implements CompilerPassInterface
{
    public const string LOCATOR_SERVICE_ID = 'flow.postgresql.command_locator';

    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasParameter('flow.postgresql.connections')) {
            return;
        }

        /** @var list<string> $connections */
        $connections = $container->getParameter('flow.postgresql.connections');

        $services = [];

        foreach ($connections as $connection) {
            foreach (['client', 'connection_parameters'] as $kind) {
                $serviceId = "flow.postgresql.{$connection}.{$kind}";

                if ($container->hasDefinition($serviceId)) {
                    $services[$serviceId] = new ServiceClosureArgument(new Reference($serviceId));
                }
            }
        }

        if ($container->hasParameter('flow.postgresql.migrations.connections')) {
            /** @var list<string> $migrationConnections */
            $migrationConnections = $container->getParameter('flow.postgresql.migrations.connections');

            foreach ($migrationConnections as $connection) {
                foreach ([
                    'configuration',
                    'migrator',
                    'store',
                    'version_resolver',
                    'generator',
                    'diff_generator',
                ] as $kind) {
                    $serviceId = "flow.postgresql.{$connection}.migrations.{$kind}";

                    if ($container->hasDefinition($serviceId)) {
                        $services[$serviceId] = new ServiceClosureArgument(new Reference($serviceId));
                    }
                }
            }
        }

        $locatorDef = new Definition(ServiceLocator::class, [$services]);
        $locatorDef->addTag('container.service_locator');
        $locatorDef->setPublic(false);

        $container->setDefinition(self::LOCATOR_SERVICE_ID, $locatorDef);
    }
}
