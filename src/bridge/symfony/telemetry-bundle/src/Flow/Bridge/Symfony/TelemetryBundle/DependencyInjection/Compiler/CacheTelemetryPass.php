<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\CacheDeferredFlushSubscriber;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\TagAwareTraceableCacheAdapter;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Cache\TraceableCacheAdapter;
use Flow\Telemetry\Telemetry;
use Symfony\Component\Cache\Adapter\TagAwareAdapterInterface;
use Symfony\Component\DependencyInjection\Argument\IteratorArgument;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

use function is_a;

final class CacheTelemetryPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
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

        $excluded = new ServiceIdPatterns($excludePools);

        $resolver = new DefinitionClassResolver($container);

        $aliasRepointer = new InterfaceAliasRepointer($container);

        $taggedServices = $container->findTaggedServiceIds('cache.pool');

        $innerPools = [];

        foreach ($taggedServices as $serviceId => $_tags) {
            if ($excluded->matches($serviceId)) {
                continue;
            }

            $serviceDefinition = $container->getDefinition($serviceId);

            if ($serviceDefinition->isAbstract()) {
                continue;
            }

            $serviceClass = $resolver->resolve($serviceDefinition);

            if ($serviceClass === null) {
                continue;
            }

            $decoratorId = $serviceId . '.flow_telemetry';
            $decoratedId = $decoratorId . '.inner';

            $isTagAware = is_a($serviceClass, TagAwareAdapterInterface::class, true);

            $adapterClass = $isTagAware ? TagAwareTraceableCacheAdapter::class : TraceableCacheAdapter::class;

            $definition = new Definition($adapterClass);
            $definition->setDecoratedService($serviceId);
            $definition->setArgument(0, new Reference($decoratedId));
            $definition->setArgument(1, new Reference(Telemetry::class));
            $definition->setArgument(2, $serviceId);

            $container->setDefinition($decoratorId, $definition);

            $aliasRepointer->repoint($serviceId, $decoratedId, $adapterClass);

            $innerPools[] = new Reference($decoratedId);
        }

        if ($this->flushDeferredEnabled($container) && $innerPools !== []) {
            $subscriber = new Definition(CacheDeferredFlushSubscriber::class);
            $subscriber->setArgument(0, new IteratorArgument($innerPools));
            $subscriber->setArgument(1, new Reference(Telemetry::class));
            $subscriber->addTag('kernel.event_subscriber');

            $container->setDefinition('flow.telemetry.cache.deferred_flush_subscriber', $subscriber);
        }
    }

    private function flushDeferredEnabled(ContainerBuilder $container): bool
    {
        return (
            $container->hasParameter('flow.telemetry.cache.flush_deferred')
            && $container->getParameter('flow.telemetry.cache.flush_deferred') === true
        );
    }
}
