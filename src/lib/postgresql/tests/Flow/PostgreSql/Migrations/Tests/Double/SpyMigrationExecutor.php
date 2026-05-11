<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use Flow\PostgreSql\Migrations\Executor\ExecutionResult;
use Flow\PostgreSql\Migrations\Executor\MigrationExecutor;
use Flow\PostgreSql\Migrations\MigrationContext;
use Flow\PostgreSql\Migrations\MigrationPlan;

final class SpyMigrationExecutor implements MigrationExecutor
{
    /**
     * @var list<MigrationPlan>
     */
    public array $executedPlans = [];

    public function execute(MigrationPlan $plan, MigrationContext $context): ExecutionResult
    {
        $this->executedPlans[] = $plan;

        return new ExecutionResult($plan->version, $plan->direction, 0, false, null);
    }
}
