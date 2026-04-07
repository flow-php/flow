<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Executor;

use Flow\PostgreSql\Migrations\{MigrationContext, MigrationPlan};

interface MigrationExecutor
{
    public function execute(MigrationPlan $plan, MigrationContext $context) : ExecutionResult;
}
