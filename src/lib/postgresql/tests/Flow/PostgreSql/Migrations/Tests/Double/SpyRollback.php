<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use Flow\PostgreSql\Migrations\{MigrationContext, Rollback};

final class SpyRollback implements Rollback
{
    public bool $rollbackCalled = false;

    public function rollback(MigrationContext $context) : void
    {
        $this->rollbackCalled = true;
    }

    public function transactional() : bool
    {
        return true;
    }
}
