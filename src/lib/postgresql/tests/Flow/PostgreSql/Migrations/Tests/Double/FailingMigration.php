<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\MigrationContext;
use RuntimeException;

final class FailingMigration implements Migration
{
    public function migrate(MigrationContext $context): void
    {
        throw new RuntimeException('Migration failed');
    }

    public function transactional(): bool
    {
        return true;
    }
}
