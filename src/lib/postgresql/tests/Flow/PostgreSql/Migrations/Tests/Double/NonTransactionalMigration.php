<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\MigrationContext;

final class NonTransactionalMigration implements Migration
{
    public bool $migrateCalled = false;

    public function migrate(MigrationContext $context): void
    {
        $this->migrateCalled = true;
    }

    public function transactional(): bool
    {
        return false;
    }
}
