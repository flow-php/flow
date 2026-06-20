<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\TracingControllerResolver;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

final class ControllerResolverTelemetryPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasParameter('flow.telemetry.http_kernel.trace_controller_resolution')) {
            return;
        }

        if ($container->getParameter('flow.telemetry.http_kernel.trace_controller_resolution') !== true) {
            return;
        }

        if (!$container->hasDefinition('controller_resolver')) {
            return;
        }

        $decoratorId = 'controller_resolver.flow_telemetry';
        $decoratedId = $decoratorId . '.inner';

        $definition = new Definition(TracingControllerResolver::class);
        $definition->setDecoratedService('controller_resolver');
        $definition->setArgument(0, new Reference($decoratedId));
        $definition->setArgument(1, new Reference(Telemetry::class));

        $container->setDefinition($decoratorId, $definition);
    }
}
