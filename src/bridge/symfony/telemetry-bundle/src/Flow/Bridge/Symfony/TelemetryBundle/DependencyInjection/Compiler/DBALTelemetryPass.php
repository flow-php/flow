<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TracingDriver;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TracingMiddleware;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TransactionSpanMode;
use Flow\Telemetry\Telemetry;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;
use Symfony\Component\DependencyInjection\Reference;

use function array_keys;
use function count;
use function is_string;
use function preg_match;

final class DBALTelemetryPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasParameter('flow.telemetry.dbal.enabled')) {
            return;
        }

        if ($container->getParameter('flow.telemetry.dbal.enabled') !== true) {
            return;
        }

        $maxSqlLength = $container->hasParameter('flow.telemetry.dbal.max_sql_length')
            ? (int) $container->getParameter('flow.telemetry.dbal.max_sql_length')
            : 1000;

        $collectMetrics = $container->hasParameter('flow.telemetry.dbal.collect_metrics')
            ? (bool) $container->getParameter('flow.telemetry.dbal.collect_metrics')
            : true;

        $includeParameters = $container->hasParameter('flow.telemetry.dbal.include_parameters')
            ? (bool) $container->getParameter('flow.telemetry.dbal.include_parameters')
            : false;

        $maxParameters = $container->hasParameter('flow.telemetry.dbal.max_parameters')
            ? (int) $container->getParameter('flow.telemetry.dbal.max_parameters')
            : 10;

        $maxParameterLength = $container->hasParameter('flow.telemetry.dbal.max_parameter_length')
            ? (int) $container->getParameter('flow.telemetry.dbal.max_parameter_length')
            : 100;

        /** @var array<string> $excludeConnections */
        $excludeConnections = $container->hasParameter('flow.telemetry.dbal.exclude_connections')
            ? $container->getParameter('flow.telemetry.dbal.exclude_connections')
            : [];

        /** @var array<string> $excludeTables */
        $excludeTables = $container->hasParameter('flow.telemetry.dbal.exclude_tables')
            ? $container->getParameter('flow.telemetry.dbal.exclude_tables')
            : [];

        $transactionSpansParam = $container->hasParameter('flow.telemetry.dbal.transaction_spans')
            ? $container->getParameter('flow.telemetry.dbal.transaction_spans')
            : 'grouped';

        $transactionSpanMode = TransactionSpanMode::from(
            is_string($transactionSpansParam) ? $transactionSpansParam : 'grouped',
        );

        $connectionNames = $this->findConnectionNames($container);

        foreach ($connectionNames as $connectionName) {
            if ($this->isExcluded($connectionName, $excludeConnections)) {
                continue;
            }

            $middlewareId = 'flow.telemetry.dbal.middleware.' . $connectionName;

            $definition = new Definition(TracingMiddleware::class);
            $definition->setArgument(0, new Reference(Telemetry::class));
            $definition->setArgument(1, TracingDriver::class);
            $definition->setArgument(2, $connectionName);
            $definition->setArgument(3, $maxSqlLength);
            $definition->setArgument(4, $excludeTables);
            $definition->setArgument(5, $transactionSpanMode);
            $definition->setArgument(6, $collectMetrics);
            $definition->setArgument(7, $includeParameters);
            $definition->setArgument(8, $maxParameters);
            $definition->setArgument(9, $maxParameterLength);
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

            foreach (array_keys($connections) as $name) {
                $connectionNames[] = $name;
            }
        }

        if (count($connectionNames) === 0 && $container->hasDefinition('doctrine.dbal.default_connection')) {
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
        $result = @preg_match($pattern, $connectionName);

        if ($result !== false) {
            return (bool) $result;
        }

        return $connectionName === $pattern;
    }
}
