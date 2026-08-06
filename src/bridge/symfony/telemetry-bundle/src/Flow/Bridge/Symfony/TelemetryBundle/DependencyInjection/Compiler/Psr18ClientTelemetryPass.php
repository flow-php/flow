<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Psr18\Telemetry\PSR18TraceableClient;
use Flow\Telemetry\Telemetry;
use Psr\Http\Client\ClientInterface;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

use function is_a;

final class Psr18ClientTelemetryPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasParameter('flow.telemetry.psr18_client.enabled')) {
            return;
        }

        if ($container->getParameter('flow.telemetry.psr18_client.enabled') !== true) {
            return;
        }

        /** @var array<string> $excludeClients */
        $excludeClients = $container->hasParameter('flow.telemetry.psr18_client.exclude_clients')
            ? $container->getParameter('flow.telemetry.psr18_client.exclude_clients')
            : [];

        $excluded = new ServiceIdPatterns($excludeClients);

        $resolver = new DefinitionClassResolver($container);

        $aliasRepointer = new InterfaceAliasRepointer($container);

        foreach ($container->getDefinitions() as $serviceId => $definition) {
            if ($excluded->matches($serviceId)) {
                continue;
            }

            if ($definition->isAbstract()) {
                continue;
            }

            $class = $resolver->resolve($definition);

            if ($class === null || $class === PSR18TraceableClient::class) {
                continue;
            }

            if (!is_a($class, ClientInterface::class, true)) {
                continue;
            }

            $decoratorId = $serviceId . '.flow_telemetry';
            $decoratedId = $decoratorId . '.inner';

            $decoratorDefinition = new Definition(PSR18TraceableClient::class);
            $decoratorDefinition->setDecoratedService($serviceId);
            $decoratorDefinition->setArgument(0, new Reference($decoratedId));
            $decoratorDefinition->setArgument(1, new Reference(Telemetry::class));

            $container->setDefinition($decoratorId, $decoratorDefinition);

            $aliasRepointer->repoint($serviceId, $decoratedId, PSR18TraceableClient::class);
        }
    }
}
