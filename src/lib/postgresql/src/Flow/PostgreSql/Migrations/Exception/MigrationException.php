<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Exception;

use Flow\PostgreSql\Migrations\Version;
use RuntimeException;
use Throwable;

use function array_map;
use function implode;
use function sprintf;

class MigrationException extends RuntimeException
{
    public static function advisoryLockNotHeld(string $name): self
    {
        return new self(sprintf(
            'The migrations lock "%s" was not held at release. Session locks do not work through a pooler in transaction mode (for example PgBouncer). Connect migrations directly to PostgreSQL, or use a lock outside the database.',
            $name,
        ));
    }

    public static function alreadyExecuted(Version $version): self
    {
        return new self(sprintf(
            'Migration "%s" is already executed. Roll it back first to execute it again.',
            $version,
        ));
    }

    public static function attributeNotFound(string $name): self
    {
        return new self(sprintf('Migration context attribute "%s" not found.', $name));
    }

    public static function configurationFileNotFound(string $fileName): self
    {
        return new self(sprintf(
            'Configuration file "%s" not found. Create a %s file or specify the path explicitly.',
            $fileName,
            $fileName,
        ));
    }

    public static function invalidConfigurationFile(string $filePath): self
    {
        return new self(sprintf('Configuration file "%s" must return an instance of Configuration.', $filePath));
    }

    public static function invalidDataMigration(string $filePath): self
    {
        return new self(sprintf('Data migration file "%s" must return an instance of Migration.', $filePath));
    }

    public static function invalidRollback(string $filePath): self
    {
        return new self(sprintf('Rollback file "%s" must return an instance of Rollback.', $filePath));
    }

    public static function invalidVersionFormat(string $version): self
    {
        return new self(sprintf(
            'Invalid migration version format: "%s". Expected non-empty alphanumeric string (max 255 chars).',
            $version,
        ));
    }

    public static function irreversibleMigration(Version $version): self
    {
        return new self(sprintf('Migration "%s" is irreversible and cannot be rolled back.', $version));
    }

    public static function lockExpired(string $name): self
    {
        return new self(sprintf(
            'The migrations lock "%s" expired before the migrations finished. Raise the lock ttl above the duration of the longest migration run.',
            $name,
        ));
    }

    public static function migrationFailed(Version $version, Throwable $previous): self
    {
        return new self(sprintf('Migration "%s" failed: %s', $version, $previous->getMessage()), 0, $previous);
    }

    public static function missingMigrationFile(string $directory): self
    {
        return new self(sprintf('Migration directory "%s" must contain migration.php.', $directory));
    }

    public static function noChangesDetected(): self
    {
        return new self('No changes detected between source and target catalog.');
    }

    public static function notExecuted(Version $version): self
    {
        return new self(sprintf('Migration "%s" is not executed, there is nothing to roll back.', $version));
    }

    public static function outOfOrder(Version $latestExecuted, Version ...$pending): self
    {
        return new self(sprintf(
            'Pending migrations "%s" are older than the latest executed migration "%s". Switch the migration mode to "all_pending" (MigrationMode::ALL_PENDING) to run them, or execute each version explicitly.',
            implode('", "', array_map(static fn(Version $version): string => (string) $version, $pending)),
            $latestExecuted,
        ));
    }

    public static function versionNotFound(Version $version): self
    {
        return new self(sprintf('Migration version "%s" not found.', $version));
    }
}
