<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpClient\TracableHttpClient;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition, Reference};

final class HttpClientTelemetryPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container) : void
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

        $taggedServices = $container->findTaggedServiceIds('http_client.client');

        foreach ($taggedServices as $serviceId => $tags) {
            if ($this->isExcluded($serviceId, $excludeClients)) {
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

    /**
     * @param array<string> $patterns
     */
    private function isExcluded(string $serviceId, array $patterns) : bool
    {
        foreach ($patterns as $pattern) {
            if ($this->matchesPattern($serviceId, $pattern)) {
                return true;
            }
        }

        return false;
    }

    private function matchesPattern(string $serviceId, string $pattern) : bool
    {
        if (\str_starts_with($pattern, '/') && \str_ends_with($pattern, '/')) {
            return (bool) \preg_match($pattern, $serviceId);
        }

        return $serviceId === $pattern;
    }
}
