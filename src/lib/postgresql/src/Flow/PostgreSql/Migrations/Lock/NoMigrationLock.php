<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Lock;

final readonly class NoMigrationLock implements MigrationLock
{
    public function acquire(string $name): bool
    {
        return true;
    }

    public function release(string $name): void {}
}
