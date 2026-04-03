<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use Flow\PostgreSql\Migrations\Executor\{ExecutionResult, MigrationExecutor};
use Flow\PostgreSql\Migrations\{MigrationContext, MigrationPlan};

final class SpyMigrationExecutor implements MigrationExecutor
{
    /**
     * @var list<MigrationPlan>
     */
    public array $executedPlans = [];

    public function execute(MigrationPlan $plan, MigrationContext $context) : ExecutionResult
    {
        $this->executedPlans[] = $plan;

        return new ExecutionResult($plan->version, $plan->direction, 0, false, null);
    }
}
