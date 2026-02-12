<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\{TagAwareTraceableCacheAdapter, TraceableCacheAdapter};
use Flow\Telemetry\Telemetry;
use Symfony\Component\Cache\Adapter\TagAwareAdapterInterface;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Definition, Reference};

final class CacheTelemetryPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container) : void
    {
        if (!$container->hasParameter('flow.telemetry.cache.enabled')) {
            return;
        }

        if ($container->getParameter('flow.telemetry.cache.enabled') !== true) {
            return;
        }

        /** @var array<string> $excludePools */
        $excludePools = $container->hasParameter('flow.telemetry.cache.exclude_pools')
            ? $container->getParameter('flow.telemetry.cache.exclude_pools')
            : [];

        $taggedServices = $container->findTaggedServiceIds('cache.pool');

        foreach ($taggedServices as $serviceId => $tags) {
            if ($this->isExcluded($serviceId, $excludePools)) {
                continue;
            }

            $serviceDefinition = $container->getDefinition($serviceId);

            if ($serviceDefinition->isAbstract()) {
                continue;
            }

            $serviceClass = $serviceDefinition->getClass();

            if ($serviceClass === null) {
                continue;
            }

            $decoratorId = $serviceId . '.flow_telemetry';
            $decoratedId = $decoratorId . '.inner';

            $isTagAware = \is_a($serviceClass, TagAwareAdapterInterface::class, true);

            $adapterClass = $isTagAware
                ? TagAwareTraceableCacheAdapter::class
                : TraceableCacheAdapter::class;

            $definition = new Definition($adapterClass);
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
        $result = @\preg_match($pattern, $serviceId);

        if ($result !== false) {
            return (bool) $result;
        }

        return $serviceId === $pattern;
    }
}
