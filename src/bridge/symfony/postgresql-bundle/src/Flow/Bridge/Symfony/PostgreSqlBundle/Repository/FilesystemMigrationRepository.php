<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Repository;

use Flow\Filesystem\{Filesystem, Path};
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\PostgreSql\Migrations\{Configuration, Migration, Rollback, Version};
use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Repository\{AvailableMigration, AvailableMigrations, MigrationRepository};

final readonly class FilesystemMigrationRepository implements MigrationRepository
{
    public function __construct(
        private Filesystem $filesystem,
        private Path $migrationsDirectory,
        private Configuration $configuration,
    ) {
    }

    public function all() : AvailableMigrations
    {
        $pattern = Path::from($this->migrationsDirectory->path() . '/*');
        $migrations = [];

        foreach ($this->filesystem->list($pattern, new KeepAll()) as $entry) {
            if ($entry->isFile()) {
                continue;
            }

            $dirName = $entry->path->basename();

            if (\preg_match('/^(\w+?)_(.+)$/', $dirName, $matches) === 1) {
                $version = Version::fromString($matches[1]);
                $name = $matches[2];
            } elseif (\preg_match('/^(\w+)$/', $dirName, $matches) === 1) {
                $version = Version::fromString($matches[1]);
                $name = $matches[1];
            } else {
                continue;
            }

            $migrations[] = $this->loadMigration($entry->path, $version, $name);
        }

        return new AvailableMigrations(...$migrations);
    }

    public function get(Version $version) : AvailableMigration
    {
        return $this->all()->get($version);
    }

    public function has(Version $version) : bool
    {
        return $this->all()->has($version);
    }

    private function loadMigration(Path $directoryPath, Version $version, string $name) : AvailableMigration
    {
        $migrationPath = Path::from($directoryPath->path() . '/' . $this->configuration->migrationFileName);

        if ($this->filesystem->status($migrationPath) === null) {
            throw MigrationException::missingMigrationFile($directoryPath->path());
        }

        $migration = require $migrationPath->path();

        if (!$migration instanceof Migration) {
            throw MigrationException::invalidDataMigration($migrationPath->path());
        }

        $rollback = null;
        $rollbackPath = Path::from($directoryPath->path() . '/' . $this->configuration->rollbackFileName);

        if ($this->filesystem->status($rollbackPath) !== null) {
            $rollback = require $rollbackPath->path();

            if (!$rollback instanceof Rollback) {
                throw MigrationException::invalidRollback($rollbackPath->path());
            }
        }

        return new AvailableMigration($version, $name, $migration, $rollback);
    }
}
