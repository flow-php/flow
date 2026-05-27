<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Repository;

use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Repository\AvailableMigrations;
use Flow\PostgreSql\Migrations\Repository\MigrationRepository;
use Flow\PostgreSql\Migrations\Rollback;
use Flow\PostgreSql\Migrations\Version;

use function preg_match;

final readonly class FilesystemMigrationRepository implements MigrationRepository
{
    public function __construct(
        private Filesystem $filesystem,
        private Path $migrationsDirectory,
        private Configuration $configuration,
    ) {}

    public function all(): AvailableMigrations
    {
        $pattern = Path::from($this->migrationsDirectory->path() . '/*');
        $migrations = [];

        foreach ($this->filesystem->list($pattern, new KeepAll()) as $entry) {
            if ($entry->isFile()) {
                continue;
            }

            $dirName = $entry->path->basename();
            $matches = [];

            if (preg_match('/^(\w+?)_(.+)$/', $dirName, $matches) === 1) {
                $version = Version::fromString($matches[1]);
                $name = $matches[2];
            } elseif (preg_match('/^(\w+)$/', $dirName, $matches) === 1) {
                $version = Version::fromString($matches[1]);
                $name = $matches[1];
            } else {
                continue;
            }

            $migrations[] = $this->loadMigration($entry->path, $version, $name);
        }

        return new AvailableMigrations(...$migrations);
    }

    public function get(Version $version): AvailableMigration
    {
        return $this->all()->get($version);
    }

    public function has(Version $version): bool
    {
        return $this->all()->has($version);
    }

    private function loadMigration(Path $directoryPath, Version $version, string $name): AvailableMigration
    {
        $migrationPath = Path::from($directoryPath->path() . '/' . $this->configuration->migrationFileName);

        if ($this->filesystem->status($migrationPath) === null) {
            throw MigrationException::missingMigrationFile($directoryPath->path());
        }

        // @mago-expect analysis:mixed-assignment
        $migration = require $migrationPath->path();

        if (!$migration instanceof Migration) {
            throw MigrationException::invalidDataMigration($migrationPath->path());
        }

        $rollback = null;
        $rollbackPath = Path::from($directoryPath->path() . '/' . $this->configuration->rollbackFileName);

        if ($this->filesystem->status($rollbackPath) !== null) {
            // @mago-expect analysis:mixed-assignment
            $rollback = require $rollbackPath->path();

            if (!$rollback instanceof Rollback) {
                throw MigrationException::invalidRollback($rollbackPath->path());
            }
        }

        return new AvailableMigration($version, $name, $migration, $rollback);
    }
}
