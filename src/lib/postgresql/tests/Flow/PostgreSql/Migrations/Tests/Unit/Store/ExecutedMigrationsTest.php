<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\Store;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\ExecutedMigration;
use Flow\PostgreSql\Migrations\Store\ExecutedMigrations;
use Flow\PostgreSql\Migrations\Version;
use PHPUnit\Framework\TestCase;

final class ExecutedMigrationsTest extends TestCase
{
    public function test_count_returns_number_of_migrations(): void
    {
        $migrations = new ExecutedMigrations(
            new ExecutedMigration(Version::fromString('20260403120000'), new \DateTimeImmutable(), 100),
            new ExecutedMigration(Version::fromString('20260403130000'), new \DateTimeImmutable(), 200),
            new ExecutedMigration(Version::fromString('20260403140000'), new \DateTimeImmutable(), 300),
        );

        static::assertCount(3, $migrations);
    }

    public function test_empty_collection_is_empty(): void
    {
        $migrations = new ExecutedMigrations();

        static::assertTrue($migrations->isEmpty());
        static::assertCount(0, $migrations);
        static::assertNull($migrations->latest());
    }

    public function test_get_returns_migration_for_existing_version(): void
    {
        $version = Version::fromString('20260403120000');
        $executed = new ExecutedMigration($version, new \DateTimeImmutable(), 100);
        $migrations = new ExecutedMigrations($executed);

        static::assertSame($executed, $migrations->get($version));
    }

    public function test_get_throws_on_missing_version(): void
    {
        $migrations = new ExecutedMigrations();

        $this->expectException(MigrationException::class);
        $migrations->get(Version::fromString('20260403120000'));
    }

    public function test_has_returns_false_for_missing_version(): void
    {
        $migrations = new ExecutedMigrations();

        static::assertFalse($migrations->has(Version::fromString('20260403120000')));
    }

    public function test_has_returns_true_for_existing_version(): void
    {
        $version = Version::fromString('20260403120000');
        $migrations = new ExecutedMigrations(new ExecutedMigration($version, new \DateTimeImmutable(), 100));

        static::assertTrue($migrations->has($version));
    }

    public function test_iteration(): void
    {
        $first = new ExecutedMigration(Version::fromString('20260403120000'), new \DateTimeImmutable(), 100);
        $second = new ExecutedMigration(Version::fromString('20260403130000'), new \DateTimeImmutable(), 200);
        $migrations = new ExecutedMigrations($first, $second);

        $items = \iterator_to_array($migrations);

        static::assertSame($first, $items[0]);
        static::assertSame($second, $items[1]);
    }

    public function test_latest_returns_last_by_version_order(): void
    {
        $earliest = new ExecutedMigration(Version::fromString('20260403120000'), new \DateTimeImmutable(), 100);
        $middle = new ExecutedMigration(Version::fromString('20260403130000'), new \DateTimeImmutable(), 200);
        $latest = new ExecutedMigration(Version::fromString('20260403140000'), new \DateTimeImmutable(), 300);

        $migrations = new ExecutedMigrations($middle, $latest, $earliest);

        static::assertNotNull($migrations->latest());
        static::assertTrue($latest->version->equals($migrations->latest()->version));
    }

    public function test_non_empty_collection_is_not_empty(): void
    {
        $migrations = new ExecutedMigrations(
            new ExecutedMigration(Version::fromString('20260403120000'), new \DateTimeImmutable(), 100),
        );

        static::assertFalse($migrations->isEmpty());
    }
}
