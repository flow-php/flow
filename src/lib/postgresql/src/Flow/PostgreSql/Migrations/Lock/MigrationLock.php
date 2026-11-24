<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Lock;

use Flow\PostgreSql\Migrations\Exception\MigrationException;

interface MigrationLock
{
    public function acquire(string $name): bool;

    /**
     * @throws MigrationException
     */
    public function release(string $name): void;
}
