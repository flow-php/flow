<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\TracingValueResolver;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

final class ArgumentValueResolverTelemetryPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasParameter('flow.telemetry.http_kernel.trace_controller_argument_resolvers')) {
            return;
        }

        if ($container->getParameter('flow.telemetry.http_kernel.trace_controller_argument_resolvers') !== true) {
            return;
        }

        foreach ($container->findTaggedServiceIds('controller.argument_value_resolver') as $serviceId => $_tags) {
            if ($container->getDefinition($serviceId)->isAbstract()) {
                continue;
            }

            $decoratorId = $serviceId . '.flow_telemetry';
            $decoratedId = $decoratorId . '.inner';

            $definition = new Definition(TracingValueResolver::class);
            $definition->setDecoratedService($serviceId);
            $definition->setArgument(0, new Reference($decoratedId));
            $definition->setArgument(1, new Reference(Telemetry::class));

            $container->setDefinition($decoratorId, $definition);
        }
    }
}
