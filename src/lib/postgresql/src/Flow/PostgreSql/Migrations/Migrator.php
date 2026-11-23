<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Executor\ExecutionResult;
use Flow\PostgreSql\Migrations\Executor\MigrationExecutor;
use Flow\PostgreSql\Migrations\Repository\MigrationRepository;
use Flow\PostgreSql\Migrations\Store\MigrationStore;
use RuntimeException;

use function array_filter;
use function array_map;
use function array_reverse;

final readonly class Migrator
{
    public function __construct(
        private MigrationRepository $repository,
        private MigrationStore $store,
        private MigrationExecutor $executor,
        private Client $client,
        private Configuration $configuration,
    ) {}

    public function executeVersion(Version $version, Direction $direction, bool $dryRun = false): ExecutionResult
    {
        $this->store->initialize();

        $available = $this->repository->get($version);
        $plan = new MigrationPlan($available->version, $available->migration, $available->rollback, $direction);
        $context = new MigrationContext($this->client, $this->configuration->attributes);

        if ($dryRun) {
            $this->client->beginTransaction();
        }

        $result = $this->executor->execute($plan, $context);

        if ($dryRun) {
            $this->client->rollBack();

            return $result;
        }

        if ($result->isSuccessful()) {
            match ($direction) {
                Direction::UP => $this->store->complete($result->version, $result->executionTimeMs),
                Direction::DOWN => $this->store->remove($result->version),
            };
        }

        return $result;
    }

    /**
     * @return list<ExecutionResult>
     */
    public function migrate(?Version $to = null, bool $dryRun = false, ?bool $allOrNothing = null): array
    {
        $this->store->initialize();

        $available = $this->repository->all();
        $executed = $this->store->executedMigrations();
        $latestExecuted = $executed->latest();

        if ($available->isEmpty() && $to === null) {
            return [];
        }

        $target = $to ?? $available->last()?->version;

        if ($target === null) {
            return [];
        }

        $plans = [];

        if ($latestExecuted === null || !$latestExecuted->version->isAfter($target)) {
            foreach ($available->upTo($target) as $migration) {
                if (!$executed->has($migration->version)) {
                    $plans[] = new MigrationPlan(
                        $migration->version,
                        $migration->migration,
                        $migration->rollback,
                        Direction::UP,
                    );
                }
            }

            if ($latestExecuted !== null && $this->configuration->mode === MigrationMode::STRICT_ORDER) {
                $outOfOrder = array_filter($plans, static fn(
                    MigrationPlan $plan,
                ): bool => $latestExecuted->version->isAfter($plan->version));

                if ($outOfOrder !== []) {
                    throw MigrationException::outOfOrder(
                        $latestExecuted->version,
                        ...array_map(static fn(MigrationPlan $plan): Version => $plan->version, $outOfOrder),
                    );
                }
            }
        } else {
            $toRollback = [];

            foreach ($executed as $em) {
                if ($em->version->isAfter($target)) {
                    $toRollback[] = $em;
                }
            }

            $toRollback = array_reverse($toRollback);

            foreach ($toRollback as $em) {
                $migration = $this->repository->get($em->version);
                $plans[] = new MigrationPlan(
                    $migration->version,
                    $migration->migration,
                    $migration->rollback,
                    Direction::DOWN,
                );
            }
        }

        if ($plans === []) {
            return [];
        }

        if ($dryRun) {
            return $this->executeWithDryRun($plans);
        }

        if ($allOrNothing ?? $this->configuration->allOrNothing) {
            return $this->executeAllOrNothing($plans);
        }

        return $this->executePlans($plans);
    }

    public function status(): MigrationStatusList
    {
        $available = $this->repository->all();
        $executed = $this->store->executedMigrations();
        $statuses = [];

        foreach ($available as $migration) {
            if ($executed->has($migration->version)) {
                $em = $executed->get($migration->version);
                $statuses[] = new MigrationStatus(
                    $migration->version,
                    $migration->name,
                    MigrationState::EXECUTED,
                    $em->executedAt,
                    $em->executionTimeMs,
                );
            } else {
                $statuses[] = new MigrationStatus($migration->version, $migration->name, MigrationState::PENDING, null);
            }
        }

        foreach ($executed as $em) {
            if (!$available->has($em->version)) {
                $statuses[] = new MigrationStatus(
                    $em->version,
                    (string) $em->version,
                    MigrationState::UNAVAILABLE,
                    $em->executedAt,
                    $em->executionTimeMs,
                );
            }
        }

        return new MigrationStatusList(...$statuses);
    }

    /**
     * @param list<MigrationPlan> $plans
     *
     * @return list<ExecutionResult>
     */
    private function executeAllOrNothing(array $plans): array
    {
        /** @var list<ExecutionResult> $results */
        $results = [];

        $this->client->transaction(function () use ($plans, &$results): void {
            $context = new MigrationContext($this->client, $this->configuration->attributes);

            foreach ($plans as $plan) {
                $result = $this->executor->execute($plan, $context);
                $results[] = $result;

                if (!$result->isSuccessful()) {
                    throw $result->error ?? new RuntimeException('Migration failed');
                }

                match ($plan->direction) {
                    Direction::UP => $this->store->complete($result->version, $result->executionTimeMs),
                    Direction::DOWN => $this->store->remove($result->version),
                };
            }
        });

        return $results;
    }

    /**
     * @param list<MigrationPlan> $plans
     *
     * @return list<ExecutionResult>
     */
    private function executePlans(array $plans): array
    {
        $context = new MigrationContext($this->client, $this->configuration->attributes);
        $results = [];

        foreach ($plans as $plan) {
            $result = $this->executor->execute($plan, $context);
            $results[] = $result;

            if (!$result->isSuccessful()) {
                break;
            }

            match ($plan->direction) {
                Direction::UP => $this->store->complete($result->version, $result->executionTimeMs),
                Direction::DOWN => $this->store->remove($result->version),
            };
        }

        return $results;
    }

    /**
     * @param list<MigrationPlan> $plans
     *
     * @return list<ExecutionResult>
     */
    private function executeWithDryRun(array $plans): array
    {
        $this->client->beginTransaction();

        try {
            $context = new MigrationContext($this->client, $this->configuration->attributes);
            $results = [];

            foreach ($plans as $plan) {
                $result = $this->executor->execute($plan, $context);
                $results[] = $result;

                if (!$result->isSuccessful()) {
                    break;
                }
            }

            return $results;
        } finally {
            $this->client->rollBack();
        }
    }
}
