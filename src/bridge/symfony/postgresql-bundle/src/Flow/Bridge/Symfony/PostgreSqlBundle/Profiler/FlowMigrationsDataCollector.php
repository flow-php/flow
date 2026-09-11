<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Profiler;

use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\MigrationState;
use Flow\PostgreSql\Migrations\Migrator;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\DataCollector\DataCollector;
use Throwable;

use function count;

/**
 * @type MigrationRow = array{version: string, name: string, state: string, executedAt: null|string, executionTimeMs: null|int}
 * @type ConfigurationData = array{tableName: string, tableSchema: string, directory: string, namespace: string, allOrNothing: bool}
 */
final class FlowMigrationsDataCollector extends DataCollector
{
    public function __construct(
        private readonly string $connection,
        private readonly Migrator $migrator,
        private readonly Configuration $configuration,
    ) {}

    public function collect(Request $request, Response $response, ?Throwable $exception = null): void
    {
        if ($this->data !== []) {
            return;
        }

        $this->data = ['connection' => $this->connection] + $this->collectStatus();
    }

    public function reset(): void
    {
        $this->data = [];
    }

    public function getName(): string
    {
        return 'flow_postgresql_migrations';
    }

    public function getConnection(): string
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['connection'] ?? '';
    }

    /**
     * @return list<MigrationRow>
     */
    public function getMigrations(): array
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['migrations'] ?? [];
    }

    /**
     * @return null|ConfigurationData
     */
    public function getConfiguration(): ?array
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['configuration'] ?? null;
    }

    public function getError(): ?string
    {
        // @mago-expect analysis:mixed-return-statement
        return $this->data['error'] ?? null;
    }

    public function getExecutedCount(): int
    {
        return (int) ($this->data['executed'] ?? 0);
    }

    public function getPendingCount(): int
    {
        return (int) ($this->data['pending'] ?? 0);
    }

    public function getUnavailableCount(): int
    {
        return (int) ($this->data['unavailable'] ?? 0);
    }

    public function getTotalCount(): int
    {
        return (int) ($this->data['total'] ?? 0);
    }

    /**
     * @return array{total: int, executed: int, pending: int, unavailable: int, migrations: list<MigrationRow>, configuration: null|ConfigurationData, error: null|string}
     */
    private function collectStatus(): array
    {
        try {
            $status = $this->migrator->status();

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
                'configuration' => $this->describeConfiguration(),
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
    private function describeConfiguration(): array
    {
        return [
            'tableName' => $this->configuration->tableName,
            'tableSchema' => $this->configuration->tableSchema,
            'directory' => $this->configuration->migrationsDirectory,
            'namespace' => $this->configuration->migrationsNamespace,
            'allOrNothing' => $this->configuration->allOrNothing,
        ];
    }
}
