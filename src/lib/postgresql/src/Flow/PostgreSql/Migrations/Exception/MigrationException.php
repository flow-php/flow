<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Exception;

use Flow\PostgreSql\Migrations\Version;
use RuntimeException;
use Throwable;

use function sprintf;

class MigrationException extends RuntimeException
{
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

    public static function versionNotFound(Version $version): self
    {
        return new self(sprintf('Migration version "%s" not found.', $version));
    }
}
