<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\{Configuration, MigrationsFactory, Migrator, Version, VersionResolver};
use Flow\PostgreSql\Migrations\Executor\MigrationExecutor;
use Flow\PostgreSql\Migrations\Generator\DiffMigrationGenerator;
use Flow\PostgreSql\Migrations\Store\MigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\{FakeCatalogProvider, FakeMigrationRepository, SpyClient, SpyMigrationGenerator};
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;

final class MigrationsFactoryTest extends TestCase
{
    public function test_create_diff_generator() : void
    {
        $factory = new MigrationsFactory(new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), \sys_get_temp_dir() . '/flow_migrations_test', 'App\\Migrations'), new FakeMigrationRepository());

        self::assertInstanceOf(DiffMigrationGenerator::class, $factory->createDiffGenerator(new SpyMigrationGenerator(Version::fromString('20260401120000'))));
    }

    public function test_create_executor() : void
    {
        $factory = new MigrationsFactory(new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), \sys_get_temp_dir() . '/flow_migrations_test', 'App\\Migrations'), new FakeMigrationRepository());

        self::assertInstanceOf(MigrationExecutor::class, $factory->createExecutor());
    }

    public function test_create_migrator() : void
    {
        $factory = new MigrationsFactory(new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), \sys_get_temp_dir() . '/flow_migrations_test', 'App\\Migrations'), new FakeMigrationRepository());

        self::assertInstanceOf(Migrator::class, $factory->createMigrator());
    }

    public function test_create_store() : void
    {
        $factory = new MigrationsFactory(new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), \sys_get_temp_dir() . '/flow_migrations_test', 'App\\Migrations'), new FakeMigrationRepository());

        self::assertInstanceOf(MigrationStore::class, $factory->createStore());
    }

    public function test_create_version_resolver() : void
    {
        $factory = new MigrationsFactory(new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), \sys_get_temp_dir() . '/flow_migrations_test', 'App\\Migrations'), new FakeMigrationRepository());

        self::assertInstanceOf(VersionResolver::class, $factory->createVersionResolver());
    }
}
