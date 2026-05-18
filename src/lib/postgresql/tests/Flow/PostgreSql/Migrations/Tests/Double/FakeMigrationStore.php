<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

use DateTimeImmutable;
use Flow\PostgreSql\Migrations\ExecutedMigration;
use Flow\PostgreSql\Migrations\Store\ExecutedMigrations;
use Flow\PostgreSql\Migrations\Store\MigrationStore;
use Flow\PostgreSql\Migrations\Version;

use function array_values;

final class FakeMigrationStore implements MigrationStore
{
    private bool $initialized = false;

    /**
     * @var array<string, ExecutedMigration>
     */
    private array $migrations = [];

    public function complete(Version $version, int $executionTimeMs): void
    {
        $this->migrations[(string) $version] = new ExecutedMigration(
            $version,
            new DateTimeImmutable(),
            $executionTimeMs,
        );
    }

    public function executedMigrations(): ExecutedMigrations
    {
        return new ExecutedMigrations(...array_values($this->migrations));
    }

    public function initialize(): void
    {
        $this->initialized = true;
    }

    public function isInitialized(): bool
    {
        return $this->initialized;
    }

    public function remove(Version $version): void
    {
        unset($this->migrations[(string) $version]);
    }

    public function reset(): void
    {
        $this->migrations = [];
    }
}
