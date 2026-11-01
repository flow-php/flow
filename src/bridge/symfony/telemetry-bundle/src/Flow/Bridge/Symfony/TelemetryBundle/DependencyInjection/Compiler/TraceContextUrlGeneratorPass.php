<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Routing\TraceContextUrlGenerator;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

/**
 * Removes the generator on router-less kernels so compilation does not break.
 */
final class TraceContextUrlGeneratorPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasDefinition('flow.telemetry.trace_context_url_generator')) {
            return;
        }

        if ($container->has('router')) {
            return;
        }

        $container->removeDefinition('flow.telemetry.trace_context_url_generator');
        $container->removeAlias(TraceContextUrlGenerator::class);
    }
}
