<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Profiler;

use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\MigrationState;
use Flow\PostgreSql\Migrations\Migrator;
use Psr\Container\ContainerInterface;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\DataCollector\DataCollector;
use Throwable;

use function array_map;
use function array_sum;
use function count;
use function Flow\Types\DSL\type_instance_of;

/**
 * @phpstan-type MigrationRow array{version: string, name: string, state: string, executedAt: null|string, executionTimeMs: null|int}
 * @phpstan-type ConfigurationData array{tableName: string, tableSchema: string, directory: string, namespace: string, allOrNothing: bool}
 * @phpstan-type ConnectionData array{total: int, executed: int, pending: int, unavailable: int, migrations: list<MigrationRow>, configuration: null|ConfigurationData, error: null|string}
 */
final class FlowMigrationsDataCollector extends DataCollector
{
    /**
     * @param list<string> $connections
     */
    public function __construct(
        private readonly ContainerInterface $locator,
        private readonly array $connections,
    ) {}

    public function collect(Request $request, Response $response, ?Throwable $exception = null): void
    {
        if ($this->data !== []) {
            return;
        }

        $connections = [];

        foreach ($this->connections as $name) {
            $connections[$name] = $this->collectConnection($name);
        }

        $this->data = ['connections' => $connections];
    }

    public function reset(): void
    {
        $this->data = [];
    }

    public function getName(): string
    {
        return 'flow_postgresql_migrations';
    }

    /**
     * @return array<string, ConnectionData>
     */
    public function getConnections(): array
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['connections'] ?? [];
    }

    public function getExecutedCount(): int
    {
        return $this->sum('executed');
    }

    public function getPendingCount(): int
    {
        return $this->sum('pending');
    }

    public function getUnavailableCount(): int
    {
        return $this->sum('unavailable');
    }

    public function getTotalCount(): int
    {
        return $this->sum('total');
    }

    /**
     * @return ConnectionData
     */
    private function collectConnection(string $name): array
    {
        try {
            $configuration = $this->describeConfiguration($name);
            $status = type_instance_of(Migrator::class)
                ->assert($this->locator->get("flow.postgresql.{$name}.migrations.migrator"))
                ->status();

            $migrations = [];
            $counts = [
                MigrationState::EXECUTED->value => 0,
                MigrationState::PENDING->value => 0,
                MigrationState::UNAVAILABLE->value => 0,
            ];

            foreach ($status as $migration) {
                $counts[$migration->state->value]++;
                $migrations[] = [
                    'version' => (string) $migration->version,
                    'name' => $migration->name,
                    'state' => $migration->state->value,
                    'executedAt' => $migration->executedAt?->format('Y-m-d H:i:s'),
                    'executionTimeMs' => $migration->executionTimeMs,
                ];
            }

            return [
                'total' => count($status),
                'executed' => $counts[MigrationState::EXECUTED->value],
                'pending' => $counts[MigrationState::PENDING->value],
                'unavailable' => $counts[MigrationState::UNAVAILABLE->value],
                'migrations' => $migrations,
                'configuration' => $configuration,
                'error' => null,
            ];
        } catch (Throwable $exception) {
            return [
                'total' => 0,
                'executed' => 0,
                'pending' => 0,
                'unavailable' => 0,
                'migrations' => [],
                'configuration' => null,
                'error' => $exception->getMessage(),
            ];
        }
    }

    /**
     * @return ConfigurationData
     */
    private function describeConfiguration(string $name): array
    {
        $configuration = type_instance_of(Configuration::class)->assert($this->locator->get(
            "flow.postgresql.{$name}.migrations.configuration",
        ));

        return [
            'tableName' => $configuration->tableName,
            'tableSchema' => $configuration->tableSchema,
            'directory' => $configuration->migrationsDirectory,
            'namespace' => $configuration->migrationsNamespace,
            'allOrNothing' => $configuration->allOrNothing,
        ];
    }

    /**
     * @param 'executed'|'pending'|'total'|'unavailable' $key
     */
    private function sum(string $key): int
    {
        return (int) array_sum(array_map(
            static fn(array $connection): int => (int) ($connection[$key] ?? 0),
            $this->getConnections(),
        ));
    }
}
