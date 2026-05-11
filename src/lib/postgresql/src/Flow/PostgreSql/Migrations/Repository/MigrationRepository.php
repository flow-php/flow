<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Repository;

use Flow\PostgreSql\Migrations\Version;

interface MigrationRepository
{
    public function all(): AvailableMigrations;

    public function get(Version $version): AvailableMigration;

    public function has(Version $version): bool;
}
