<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\Repository;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\MigrationContext;
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Repository\AvailableMigrations;
use Flow\PostgreSql\Migrations\Version;
use PHPUnit\Framework\TestCase;

final class AvailableMigrationsTest extends TestCase
{
    public function stubMigration(): Migration
    {
        return new class implements Migration {
            public function migrate(MigrationContext $context): void {}

            public function transactional(): bool
            {
                return true;
            }
        };
    }

    public function test_after_filters_migrations_strictly_after_given_version(): void
    {
        $v1 = Version::fromString('20260401120000');
        $v2 = Version::fromString('20260402120000');
        $v3 = Version::fromString('20260403120000');

        $migrations = new AvailableMigrations(
            new AvailableMigration($v1, 'first', $this->stubMigration(), null),
            new AvailableMigration($v2, 'second', $this->stubMigration(), null),
            new AvailableMigration($v3, 'third', $this->stubMigration(), null),
        );

        $after = $migrations->after($v2);

        static::assertCount(1, $after);
        static::assertTrue($after->has($v3));
        static::assertFalse($after->has($v1));
        static::assertFalse($after->has($v2));
    }

    public function test_after_returns_empty_when_no_migrations_after(): void
    {
        $v1 = Version::fromString('20260401120000');

        $migrations = new AvailableMigrations(new AvailableMigration($v1, 'first', $this->stubMigration(), null));

        static::assertTrue($migrations->after($v1)->isEmpty());
    }

    public function test_count_returns_number_of_migrations(): void
    {
        $migrations = new AvailableMigrations(
            new AvailableMigration(Version::fromString('20260401120000'), 'first', $this->stubMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'second', $this->stubMigration(), null),
            new AvailableMigration(Version::fromString('20260403120000'), 'third', $this->stubMigration(), null),
        );

        static::assertCount(3, $migrations);
    }

    public function test_empty_collection_is_empty(): void
    {
        $migrations = new AvailableMigrations();

        static::assertTrue($migrations->isEmpty());
        static::assertCount(0, $migrations);
        static::assertNull($migrations->first());
        static::assertNull($migrations->last());
    }

    public function test_first_returns_earliest_version(): void
    {
        $v1 = Version::fromString('20260401120000');
        $v2 = Version::fromString('20260402120000');

        $migrations = new AvailableMigrations(
            new AvailableMigration($v2, 'second', $this->stubMigration(), null),
            new AvailableMigration($v1, 'first', $this->stubMigration(), null),
        );

        static::assertNotNull($migrations->first());
        static::assertTrue($v1->equals($migrations->first()->version));
    }

    public function test_get_returns_migration_for_existing_version(): void
    {
        $version = Version::fromString('20260401120000');
        $available = new AvailableMigration($version, 'test', $this->stubMigration(), null);
        $migrations = new AvailableMigrations($available);

        static::assertSame($available, $migrations->get($version));
    }

    public function test_get_throws_on_missing_version(): void
    {
        $migrations = new AvailableMigrations();

        $this->expectException(MigrationException::class);
        $migrations->get(Version::fromString('20260401120000'));
    }

    public function test_has_returns_false_for_missing_version(): void
    {
        $migrations = new AvailableMigrations();

        static::assertFalse($migrations->has(Version::fromString('20260401120000')));
    }

    public function test_has_returns_true_for_existing_version(): void
    {
        $version = Version::fromString('20260401120000');
        $migrations = new AvailableMigrations(new AvailableMigration($version, 'test', $this->stubMigration(), null));

        static::assertTrue($migrations->has($version));
    }

    public function test_iteration_is_sorted_by_version(): void
    {
        $v1 = Version::fromString('20260401120000');
        $v2 = Version::fromString('20260402120000');
        $v3 = Version::fromString('20260403120000');

        $migrations = new AvailableMigrations(
            new AvailableMigration($v3, 'third', $this->stubMigration(), null),
            new AvailableMigration($v1, 'first', $this->stubMigration(), null),
            new AvailableMigration($v2, 'second', $this->stubMigration(), null),
        );

        $items = \iterator_to_array($migrations);

        static::assertTrue($v1->equals($items[0]->version));
        static::assertTrue($v2->equals($items[1]->version));
        static::assertTrue($v3->equals($items[2]->version));
    }

    public function test_last_returns_latest_version(): void
    {
        $v1 = Version::fromString('20260401120000');
        $v2 = Version::fromString('20260402120000');

        $migrations = new AvailableMigrations(
            new AvailableMigration($v1, 'first', $this->stubMigration(), null),
            new AvailableMigration($v2, 'second', $this->stubMigration(), null),
        );

        static::assertNotNull($migrations->last());
        static::assertTrue($v2->equals($migrations->last()->version));
    }

    public function test_non_empty_collection_is_not_empty(): void
    {
        $migrations = new AvailableMigrations(
            new AvailableMigration(Version::fromString('20260401120000'), 'test', $this->stubMigration(), null),
        );

        static::assertFalse($migrations->isEmpty());
    }

    public function test_up_to_filters_migrations_up_to_and_including_version(): void
    {
        $v1 = Version::fromString('20260401120000');
        $v2 = Version::fromString('20260402120000');
        $v3 = Version::fromString('20260403120000');

        $migrations = new AvailableMigrations(
            new AvailableMigration($v1, 'first', $this->stubMigration(), null),
            new AvailableMigration($v2, 'second', $this->stubMigration(), null),
            new AvailableMigration($v3, 'third', $this->stubMigration(), null),
        );

        $upTo = $migrations->upTo($v2);

        static::assertCount(2, $upTo);
        static::assertTrue($upTo->has($v1));
        static::assertTrue($upTo->has($v2));
        static::assertFalse($upTo->has($v3));
    }

    public function test_up_to_returns_empty_when_all_versions_are_after(): void
    {
        $v1 = Version::fromString('20260401120000');
        $earlier = Version::fromString('20260101000000');

        $migrations = new AvailableMigrations(new AvailableMigration($v1, 'first', $this->stubMigration(), null));

        static::assertTrue($migrations->upTo($earlier)->isEmpty());
    }
}
