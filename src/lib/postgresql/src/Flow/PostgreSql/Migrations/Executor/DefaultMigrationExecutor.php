<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Executor;

use Flow\PostgreSql\Migrations\{Direction, MigrationContext, MigrationPlan};
use Flow\PostgreSql\Migrations\Exception\MigrationException;

final readonly class DefaultMigrationExecutor implements MigrationExecutor
{
    public function execute(MigrationPlan $plan, MigrationContext $context) : ExecutionResult
    {
        $start = \hrtime(true);

        try {
            match ($plan->direction) {
                Direction::UP => $this->executeUp($plan, $context),
                Direction::DOWN => $this->executeDown($plan, $context),
            };
        } catch (\Throwable $e) {
            return new ExecutionResult(
                $plan->version,
                $plan->direction,
                (int) ((\hrtime(true) - $start) / 1_000_000),
                false,
                $e,
            );
        }

        return new ExecutionResult(
            $plan->version,
            $plan->direction,
            (int) ((\hrtime(true) - $start) / 1_000_000),
            false,
            null,
        );
    }

    private function executeDown(MigrationPlan $plan, MigrationContext $context) : void
    {
        if ($plan->rollback === null) {
            throw MigrationException::irreversibleMigration($plan->version);
        }

        if ($plan->rollback->transactional()) {
            $context->client->transaction(static function () use ($plan, $context) : void {
                $plan->rollback->rollback($context);
            });

            return;
        }

        $plan->rollback->rollback($context);
    }

    private function executeUp(MigrationPlan $plan, MigrationContext $context) : void
    {
        if ($plan->migration->transactional()) {
            $context->client->transaction(static function () use ($plan, $context) : void {
                $plan->migration->migrate($context);
            });

            return;
        }

        $plan->migration->migrate($context);
    }
}
