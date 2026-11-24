<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\Exception;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Version;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class MigrationExceptionTest extends TestCase
{
    public function test_advisory_lock_not_held(): void
    {
        static::assertSame(
            'The migrations lock "public.flow_migrations" was not held at release. Session locks do not work through a pooler in transaction mode (for example PgBouncer). Connect migrations directly to PostgreSQL, or use a lock outside the database.',
            MigrationException::advisoryLockNotHeld('public.flow_migrations')->getMessage(),
        );
    }

    public function test_already_executed(): void
    {
        static::assertSame(
            'Migration "20260401120000" is already executed. Roll it back first to execute it again.',
            MigrationException::alreadyExecuted(Version::fromString('20260401120000'))->getMessage(),
        );
    }

    public function test_attribute_not_found(): void
    {
        $exception = MigrationException::attributeNotFound('container');

        static::assertSame('Migration context attribute "container" not found.', $exception->getMessage());
    }

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

    public function test_lock_expired(): void
    {
        static::assertSame(
            'The migrations lock "public.flow_migrations" expired before the migrations finished. Raise the lock ttl above the duration of the longest migration run.',
            MigrationException::lockExpired('public.flow_migrations')->getMessage(),
        );
    }

    public function test_migration_failed(): void
    {
        $previous = new RuntimeException('Connection lost');
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

    public function test_not_executed(): void
    {
        static::assertSame(
            'Migration "20260401120000" is not executed, there is nothing to roll back.',
            MigrationException::notExecuted(Version::fromString('20260401120000'))->getMessage(),
        );
    }

    public function test_out_of_order(): void
    {
        static::assertSame(
            'Pending migrations "20260101000000", "20260102000000" are older than the latest executed migration "20260103000000". Switch the migration mode to "all_pending" (MigrationMode::ALL_PENDING) to run them, or execute each version explicitly.',
            MigrationException::outOfOrder(
                Version::fromString('20260103000000'),
                Version::fromString('20260101000000'),
                Version::fromString('20260102000000'),
            )->getMessage(),
        );
    }

    public function test_version_not_found(): void
    {
        $exception = MigrationException::versionNotFound(Version::fromString('20260403120000'));

        static::assertSame('Migration version "20260403120000" not found.', $exception->getMessage());
    }
}
