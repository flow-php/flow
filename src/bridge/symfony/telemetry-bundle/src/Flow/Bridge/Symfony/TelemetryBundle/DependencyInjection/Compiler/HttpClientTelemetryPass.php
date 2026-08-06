<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient\TracableHttpClient;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

final class HttpClientTelemetryPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasParameter('flow.telemetry.http_client.enabled')) {
            return;
        }

        if ($container->getParameter('flow.telemetry.http_client.enabled') !== true) {
            return;
        }

        /** @var array<string> $excludeClients */
        $excludeClients = $container->hasParameter('flow.telemetry.http_client.exclude_clients')
            ? $container->getParameter('flow.telemetry.http_client.exclude_clients')
            : [];

        $excluded = new ServiceIdPatterns($excludeClients);

        $taggedServices = $container->findTaggedServiceIds('http_client.client');

        foreach ($taggedServices as $serviceId => $_tags) {
            if ($excluded->matches($serviceId)) {
                continue;
            }

            $decoratorId = $serviceId . '.flow_telemetry';
            $decoratedId = $decoratorId . '.inner';

            $definition = new Definition(TracableHttpClient::class);
            $definition->setDecoratedService($serviceId);
            $definition->setArgument(0, new Reference($decoratedId));
            $definition->setArgument(1, new Reference(Telemetry::class));
            $definition->setArgument(2, $serviceId);

            $container->setDefinition($decoratorId, $definition);
        }
    }
}
