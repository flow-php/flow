<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\Exception;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Version;
use PHPUnit\Framework\TestCase;

final class MigrationExceptionTest extends TestCase
{
    public function test_configuration_file_not_found(): void
    {
        $exception = MigrationException::configurationFileNotFound('migrations.php');

        static::assertStringContainsString('migrations.php', $exception->getMessage());
        static::assertStringContainsString('not found', $exception->getMessage());
    }

    public function test_invalid_configuration_file(): void
    {
        $exception = MigrationException::invalidConfigurationFile('/path/to/config.php');

        static::assertStringContainsString('/path/to/config.php', $exception->getMessage());
        static::assertStringContainsString('Configuration', $exception->getMessage());
    }

    public function test_invalid_data_migration(): void
    {
        $exception = MigrationException::invalidDataMigration('/path/to/file.php');

        static::assertSame(
            'Data migration file "/path/to/file.php" must return an instance of Migration.',
            $exception->getMessage(),
        );
    }

    public function test_invalid_rollback(): void
    {
        $exception = MigrationException::invalidRollback('/path/to/file.php');

        static::assertSame(
            'Rollback file "/path/to/file.php" must return an instance of Rollback.',
            $exception->getMessage(),
        );
    }

    public function test_invalid_version_format(): void
    {
        $exception = MigrationException::invalidVersionFormat('abc');

        static::assertSame(
            'Invalid migration version format: "abc". Expected non-empty alphanumeric string (max 255 chars).',
            $exception->getMessage(),
        );
    }

    public function test_irreversible_migration(): void
    {
        $exception = MigrationException::irreversibleMigration(Version::fromString('20260403120000'));

        static::assertSame(
            'Migration "20260403120000" is irreversible and cannot be rolled back.',
            $exception->getMessage(),
        );
    }

    public function test_migration_failed(): void
    {
        $previous = new \RuntimeException('Connection lost');
        $exception = MigrationException::migrationFailed(Version::fromString('20260403120000'), $previous);

        static::assertSame('Migration "20260403120000" failed: Connection lost', $exception->getMessage());
        static::assertSame($previous, $exception->getPrevious());
    }

    public function test_missing_migration_file(): void
    {
        $exception = MigrationException::missingMigrationFile('/path/to/dir');

        static::assertSame('Migration directory "/path/to/dir" must contain migration.php.', $exception->getMessage());
    }

    public function test_no_changes_detected(): void
    {
        $exception = MigrationException::noChangesDetected();

        static::assertSame('No changes detected between source and target catalog.', $exception->getMessage());
    }

    public function test_version_not_found(): void
    {
        $exception = MigrationException::versionNotFound(Version::fromString('20260403120000'));

        static::assertSame('Migration version "20260403120000" not found.', $exception->getMessage());
    }
}
