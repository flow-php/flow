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

use function class_exists;
use function is_a;
use function preg_match;

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

        foreach ($container->getDefinitions() as $serviceId => $definition) {
            if ($this->isExcluded($serviceId, $excludeClients)) {
                continue;
            }

            if (!$this->implementsPsr18Interface($definition)) {
                continue;
            }

            $decoratorId = $serviceId . '.flow_telemetry';
            $decoratedId = $decoratorId . '.inner';

            $decoratorDefinition = new Definition(PSR18TraceableClient::class);
            $decoratorDefinition->setDecoratedService($serviceId);
            $decoratorDefinition->setArgument(0, new Reference($decoratedId));
            $decoratorDefinition->setArgument(1, new Reference(Telemetry::class));

            $container->setDefinition($decoratorId, $decoratorDefinition);
        }
    }

    private function implementsPsr18Interface(Definition $definition): bool
    {
        $class = $definition->getClass();

        if ($class === null) {
            return false;
        }

        if ($class === PSR18TraceableClient::class) {
            return false;
        }

        if (!class_exists($class)) {
            return false;
        }

        return is_a($class, ClientInterface::class, true);
    }

    /**
     * @param array<string> $patterns
     */
    private function isExcluded(string $serviceId, array $patterns): bool
    {
        foreach ($patterns as $pattern) {
            if ($this->matchesPattern($serviceId, $pattern)) {
                return true;
            }
        }

        return false;
    }

    private function matchesPattern(string $serviceId, string $pattern): bool
    {
        $result = @preg_match($pattern, $serviceId);

        if ($result !== false) {
            return (bool) $result;
        }

        return $serviceId === $pattern;
    }
}
