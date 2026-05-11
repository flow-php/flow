<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use Flow\PostgreSql\Migrations\MigrationContext;
use Flow\PostgreSql\Migrations\Rollback;

final class NonTransactionalRollback implements Rollback
{
    public bool $rollbackCalled = false;

    public function rollback(MigrationContext $context): void
    {
        $this->rollbackCalled = true;
    }

    public function transactional(): bool
    {
        return false;
    }
}
