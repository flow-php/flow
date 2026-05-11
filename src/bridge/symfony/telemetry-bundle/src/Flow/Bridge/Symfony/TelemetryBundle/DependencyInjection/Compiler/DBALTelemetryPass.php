<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\V3\TracingDriver as V3TracingDriver;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\V4\TracingDriver as V4TracingDriver;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

final class DBALTelemetryPass implements CompilerPassInterface
{
    private const string VERSION_AWARE_PLATFORM_DRIVER = 'Doctrine\\DBAL\\VersionAwarePlatformDriver';

    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasParameter('flow.telemetry.dbal.enabled')) {
            return;
        }

        if ($container->getParameter('flow.telemetry.dbal.enabled') !== true) {
            return;
        }

        $logSql = $container->hasParameter('flow.telemetry.dbal.log_sql')
            ? (bool) $container->getParameter('flow.telemetry.dbal.log_sql')
            : true;

        $maxSqlLength = $container->hasParameter('flow.telemetry.dbal.max_sql_length')
            ? (int) $container->getParameter('flow.telemetry.dbal.max_sql_length')
            : 1000;

        /** @var array<string> $excludeConnections */
        $excludeConnections = $container->hasParameter('flow.telemetry.dbal.exclude_connections')
            ? $container->getParameter('flow.telemetry.dbal.exclude_connections')
            : [];

        $driverClass = $this->resolveDriverClass();
        $connectionNames = $this->findConnectionNames($container);

        foreach ($connectionNames as $connectionName) {
            if ($this->isExcluded($connectionName, $excludeConnections)) {
                continue;
            }

            $middlewareId = 'flow.telemetry.dbal.middleware.' . $connectionName;

            $definition = new Definition(TracingMiddleware::class);
            $definition->setArgument(0, new Reference(Telemetry::class));
            $definition->setArgument(1, $driverClass);
            $definition->setArgument(2, $connectionName);
            $definition->setArgument(3, $logSql);
            $definition->setArgument(4, $maxSqlLength);
            $definition->addTag('doctrine.middleware', ['connection' => $connectionName]);

            $container->setDefinition($middlewareId, $definition);
        }
    }

    /**
     * @return array<string>
     */
    private function findConnectionNames(ContainerBuilder $container): array
    {
        $connectionNames = [];

        if ($container->hasParameter('doctrine.connections')) {
            /** @var array<string, string> $connections */
            $connections = $container->getParameter('doctrine.connections');

            foreach (\array_keys($connections) as $name) {
                $connectionNames[] = $name;
            }
        }

        if (\count($connectionNames) === 0 && $container->hasDefinition('doctrine.dbal.default_connection')) {
            $connectionNames[] = 'default';
        }

        return $connectionNames;
    }

    /**
     * @param array<string> $patterns
     */
    private function isExcluded(string $connectionName, array $patterns): bool
    {
        foreach ($patterns as $pattern) {
            if ($this->matchesPattern($connectionName, $pattern)) {
                return true;
            }
        }

        return false;
    }

    private function matchesPattern(string $connectionName, string $pattern): bool
    {
        $result = @\preg_match($pattern, $connectionName);

        if ($result !== false) {
            return (bool) $result;
        }

        return $connectionName === $pattern;
    }

    /**
     * @return class-string
     */
    private function resolveDriverClass(): string
    {
        if (\interface_exists(self::VERSION_AWARE_PLATFORM_DRIVER)) {
            return V3TracingDriver::class;
        }

        return V4TracingDriver::class;
    }
}
