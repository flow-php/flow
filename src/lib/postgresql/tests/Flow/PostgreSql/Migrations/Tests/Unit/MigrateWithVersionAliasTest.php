<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\{Configuration, Direction, Migrator, Version, VersionResolver};
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Tests\Double\{FakeCatalogProvider, FakeMigrationRepository, FakeMigrationStore, SpyClient, SpyMigration, SpyMigrationExecutor, SpyRollback};
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;

final class MigrateWithVersionAliasTest extends TestCase
{
    public function test_first_migrates_up_to_first_available_version() : void
    {
        $store = new FakeMigrationStore();
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402120000'), 'create_posts', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260403120000'), 'create_comments', new SpyMigration(), new SpyRollback()),
        );
        $client = new SpyClient();
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'));
        $resolver = new VersionResolver($repository, $store);

        $results = $migrator->migrate($resolver->resolve('first'));

        self::assertCount(1, $results);
        self::assertSame(Direction::UP, $results[0]->direction);
        self::assertTrue($results[0]->version->equals(Version::fromString('20260401120000')));
        self::assertCount(1, $store->executedMigrations());
    }

    public function test_first_rolls_back_to_first_when_all_executed() : void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 10);
        $store->complete(Version::fromString('20260402120000'), 10);
        $store->complete(Version::fromString('20260403120000'), 10);

        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402120000'), 'create_posts', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260403120000'), 'create_comments', new SpyMigration(), new SpyRollback()),
        );
        $client = new SpyClient();
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'));
        $resolver = new VersionResolver($repository, $store);

        $results = $migrator->migrate($resolver->resolve('first'));

        self::assertCount(2, $results);
        self::assertSame(Direction::DOWN, $results[0]->direction);
        self::assertTrue($results[0]->version->equals(Version::fromString('20260403120000')));
        self::assertSame(Direction::DOWN, $results[1]->direction);
        self::assertTrue($results[1]->version->equals(Version::fromString('20260402120000')));
        self::assertCount(1, $store->executedMigrations());
        self::assertTrue($store->executedMigrations()->has(Version::fromString('20260401120000')));
    }

    public function test_full_lifecycle_latest_then_prev_then_next_then_first() : void
    {
        $store = new FakeMigrationStore();
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402120000'), 'create_posts', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260403120000'), 'create_comments', new SpyMigration(), new SpyRollback()),
        );
        $client = new SpyClient();
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'));
        $resolver = new VersionResolver($repository, $store);

        $results = $migrator->migrate($resolver->resolve('latest'));
        self::assertCount(3, $results);
        self::assertCount(3, $store->executedMigrations());

        $results = $migrator->migrate($resolver->resolve('prev'));
        self::assertCount(1, $results);
        self::assertSame(Direction::DOWN, $results[0]->direction);
        self::assertCount(2, $store->executedMigrations());

        $results = $migrator->migrate($resolver->resolve('next'));
        self::assertCount(1, $results);
        self::assertSame(Direction::UP, $results[0]->direction);
        self::assertCount(3, $store->executedMigrations());

        $results = $migrator->migrate($resolver->resolve('first'));
        self::assertCount(2, $results);
        self::assertSame(Direction::DOWN, $results[0]->direction);
        self::assertSame(Direction::DOWN, $results[1]->direction);
        self::assertCount(1, $store->executedMigrations());
        self::assertTrue($store->executedMigrations()->has(Version::fromString('20260401120000')));
    }

    public function test_latest_migrates_all_from_scratch() : void
    {
        $store = new FakeMigrationStore();
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402120000'), 'create_posts', new SpyMigration(), new SpyRollback()),
        );
        $client = new SpyClient();
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'));
        $resolver = new VersionResolver($repository, $store);

        $results = $migrator->migrate($resolver->resolve('latest'));

        self::assertCount(2, $results);
        self::assertCount(2, $store->executedMigrations());
    }

    public function test_latest_migrates_all_pending() : void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 10);

        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402120000'), 'create_posts', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260403120000'), 'create_comments', new SpyMigration(), new SpyRollback()),
        );
        $client = new SpyClient();
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'));
        $resolver = new VersionResolver($repository, $store);

        $results = $migrator->migrate($resolver->resolve('latest'));

        self::assertCount(2, $results);
        self::assertSame(Direction::UP, $results[0]->direction);
        self::assertTrue($results[0]->version->equals(Version::fromString('20260402120000')));
        self::assertSame(Direction::UP, $results[1]->direction);
        self::assertTrue($results[1]->version->equals(Version::fromString('20260403120000')));
        self::assertCount(3, $store->executedMigrations());
    }

    public function test_latest_returns_empty_when_already_up_to_date() : void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 10);
        $store->complete(Version::fromString('20260402120000'), 10);

        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402120000'), 'create_posts', new SpyMigration(), new SpyRollback()),
        );
        $client = new SpyClient();
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'));
        $resolver = new VersionResolver($repository, $store);

        $results = $migrator->migrate($resolver->resolve('latest'));

        self::assertCount(0, $results);
    }

    public function test_next_migrates_first_when_nothing_executed() : void
    {
        $store = new FakeMigrationStore();
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402120000'), 'create_posts', new SpyMigration(), new SpyRollback()),
        );
        $client = new SpyClient();
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'));
        $resolver = new VersionResolver($repository, $store);

        $results = $migrator->migrate($resolver->resolve('next'));

        self::assertCount(1, $results);
        self::assertTrue($results[0]->version->equals(Version::fromString('20260401120000')));
    }

    public function test_next_migrates_one_step_forward() : void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 10);

        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402120000'), 'create_posts', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260403120000'), 'create_comments', new SpyMigration(), new SpyRollback()),
        );
        $client = new SpyClient();
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'));
        $resolver = new VersionResolver($repository, $store);

        $results = $migrator->migrate($resolver->resolve('next'));

        self::assertCount(1, $results);
        self::assertSame(Direction::UP, $results[0]->direction);
        self::assertTrue($results[0]->version->equals(Version::fromString('20260402120000')));
        self::assertCount(2, $store->executedMigrations());
    }

    public function test_prev_rolls_back_everything_when_only_one_executed() : void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 10);

        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402120000'), 'create_posts', new SpyMigration(), new SpyRollback()),
        );
        $client = new SpyClient();
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'));
        $resolver = new VersionResolver($repository, $store);

        $results = $migrator->migrate($resolver->resolve('prev'));

        self::assertCount(1, $results);
        self::assertSame(Direction::DOWN, $results[0]->direction);
        self::assertTrue($results[0]->version->equals(Version::fromString('20260401120000')));
        self::assertCount(0, $store->executedMigrations());
    }

    public function test_prev_rolls_back_one_step() : void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 10);
        $store->complete(Version::fromString('20260402120000'), 10);
        $store->complete(Version::fromString('20260403120000'), 10);

        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402120000'), 'create_posts', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260403120000'), 'create_comments', new SpyMigration(), new SpyRollback()),
        );
        $client = new SpyClient();
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'));
        $resolver = new VersionResolver($repository, $store);

        $results = $migrator->migrate($resolver->resolve('prev'));

        self::assertCount(1, $results);
        self::assertSame(Direction::DOWN, $results[0]->direction);
        self::assertTrue($results[0]->version->equals(Version::fromString('20260403120000')));
        self::assertCount(2, $store->executedMigrations());
        self::assertTrue($store->executedMigrations()->has(Version::fromString('20260401120000')));
        self::assertTrue($store->executedMigrations()->has(Version::fromString('20260402120000')));
        self::assertFalse($store->executedMigrations()->has(Version::fromString('20260403120000')));
    }

    public function test_prev_then_next_is_round_trip() : void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 10);
        $store->complete(Version::fromString('20260402120000'), 10);

        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402120000'), 'create_posts', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260403120000'), 'create_comments', new SpyMigration(), new SpyRollback()),
        );
        $client = new SpyClient();
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'));
        $resolver = new VersionResolver($repository, $store);

        $migrator->migrate($resolver->resolve('prev'));
        self::assertCount(1, $store->executedMigrations());
        self::assertTrue($store->executedMigrations()->has(Version::fromString('20260401120000')));

        $results = $migrator->migrate($resolver->resolve('next'));
        self::assertCount(1, $results);
        self::assertSame(Direction::UP, $results[0]->direction);
        self::assertTrue($results[0]->version->equals(Version::fromString('20260402120000')));
        self::assertCount(2, $store->executedMigrations());
    }
}
