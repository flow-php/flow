<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Repository\AvailableMigrations;
use Flow\PostgreSql\Migrations\Repository\MigrationRepository;
use Flow\PostgreSql\Migrations\Version;

final class FakeMigrationRepository implements MigrationRepository
{
    private AvailableMigrations $migrations;

    public function __construct(AvailableMigration ...$migrations)
    {
        $this->migrations = new AvailableMigrations(...$migrations);
    }

    public function all(): AvailableMigrations
    {
        return $this->migrations;
    }

    public function get(Version $version): AvailableMigration
    {
        return $this->migrations->get($version);
    }

    public function has(Version $version): bool
    {
        return $this->migrations->has($version);
    }
}
