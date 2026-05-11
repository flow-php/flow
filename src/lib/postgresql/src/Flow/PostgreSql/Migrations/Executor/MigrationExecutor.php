<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Executor;

use Flow\PostgreSql\Migrations\MigrationContext;
use Flow\PostgreSql\Migrations\MigrationPlan;

interface MigrationExecutor
{
    public function execute(MigrationPlan $plan, MigrationContext $context): ExecutionResult;
}
