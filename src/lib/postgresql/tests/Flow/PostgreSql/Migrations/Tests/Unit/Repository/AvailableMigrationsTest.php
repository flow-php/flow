<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\Repository;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\{Migration, MigrationContext, Version};
use Flow\PostgreSql\Migrations\Repository\{AvailableMigration, AvailableMigrations};
use PHPUnit\Framework\TestCase;

final class AvailableMigrationsTest extends TestCase
{
    public function stubMigration() : Migration
    {
        return new class implements Migration {
            public function migrate(MigrationContext $context) : void
            {
            }

            public function transactional() : bool
            {
                return true;
            }
        };
    }

    public function test_after_filters_migrations_strictly_after_given_version() : void
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

        self::assertCount(1, $after);
        self::assertTrue($after->has($v3));
        self::assertFalse($after->has($v1));
        self::assertFalse($after->has($v2));
    }

    public function test_after_returns_empty_when_no_migrations_after() : void
    {
        $v1 = Version::fromString('20260401120000');

        $migrations = new AvailableMigrations(
            new AvailableMigration($v1, 'first', $this->stubMigration(), null),
        );

        self::assertTrue($migrations->after($v1)->isEmpty());
    }

    public function test_count_returns_number_of_migrations() : void
    {
        $migrations = new AvailableMigrations(
            new AvailableMigration(Version::fromString('20260401120000'), 'first', $this->stubMigration(), null),
            new AvailableMigration(Version::fromString('20260402120000'), 'second', $this->stubMigration(), null),
            new AvailableMigration(Version::fromString('20260403120000'), 'third', $this->stubMigration(), null),
        );

        self::assertCount(3, $migrations);
    }

    public function test_empty_collection_is_empty() : void
    {
        $migrations = new AvailableMigrations();

        self::assertTrue($migrations->isEmpty());
        self::assertCount(0, $migrations);
        self::assertNull($migrations->first());
        self::assertNull($migrations->last());
    }

    public function test_first_returns_earliest_version() : void
    {
        $v1 = Version::fromString('20260401120000');
        $v2 = Version::fromString('20260402120000');

        $migrations = new AvailableMigrations(
            new AvailableMigration($v2, 'second', $this->stubMigration(), null),
            new AvailableMigration($v1, 'first', $this->stubMigration(), null),
        );

        self::assertNotNull($migrations->first());
        self::assertTrue($v1->equals($migrations->first()->version));
    }

    public function test_get_returns_migration_for_existing_version() : void
    {
        $version = Version::fromString('20260401120000');
        $available = new AvailableMigration($version, 'test', $this->stubMigration(), null);
        $migrations = new AvailableMigrations($available);

        self::assertSame($available, $migrations->get($version));
    }

    public function test_get_throws_on_missing_version() : void
    {
        $migrations = new AvailableMigrations();

        $this->expectException(MigrationException::class);
        $migrations->get(Version::fromString('20260401120000'));
    }

    public function test_has_returns_false_for_missing_version() : void
    {
        $migrations = new AvailableMigrations();

        self::assertFalse($migrations->has(Version::fromString('20260401120000')));
    }

    public function test_has_returns_true_for_existing_version() : void
    {
        $version = Version::fromString('20260401120000');
        $migrations = new AvailableMigrations(
            new AvailableMigration($version, 'test', $this->stubMigration(), null),
        );

        self::assertTrue($migrations->has($version));
    }

    public function test_iteration_is_sorted_by_version() : void
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

        self::assertTrue($v1->equals($items[0]->version));
        self::assertTrue($v2->equals($items[1]->version));
        self::assertTrue($v3->equals($items[2]->version));
    }

    public function test_last_returns_latest_version() : void
    {
        $v1 = Version::fromString('20260401120000');
        $v2 = Version::fromString('20260402120000');

        $migrations = new AvailableMigrations(
            new AvailableMigration($v1, 'first', $this->stubMigration(), null),
            new AvailableMigration($v2, 'second', $this->stubMigration(), null),
        );

        self::assertNotNull($migrations->last());
        self::assertTrue($v2->equals($migrations->last()->version));
    }

    public function test_non_empty_collection_is_not_empty() : void
    {
        $migrations = new AvailableMigrations(
            new AvailableMigration(Version::fromString('20260401120000'), 'test', $this->stubMigration(), null),
        );

        self::assertFalse($migrations->isEmpty());
    }

    public function test_up_to_filters_migrations_up_to_and_including_version() : void
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

        self::assertCount(2, $upTo);
        self::assertTrue($upTo->has($v1));
        self::assertTrue($upTo->has($v2));
        self::assertFalse($upTo->has($v3));
    }

    public function test_up_to_returns_empty_when_all_versions_are_after() : void
    {
        $v1 = Version::fromString('20260401120000');
        $earlier = Version::fromString('20260101000000');

        $migrations = new AvailableMigrations(
            new AvailableMigration($v1, 'first', $this->stubMigration(), null),
        );

        self::assertTrue($migrations->upTo($earlier)->isEmpty());
    }
}
