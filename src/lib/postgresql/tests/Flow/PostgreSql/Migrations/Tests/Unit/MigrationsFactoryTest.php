<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Executor\MigrationExecutor;
use Flow\PostgreSql\Migrations\Generator\DiffMigrationGenerator;
use Flow\PostgreSql\Migrations\MigrationsFactory;
use Flow\PostgreSql\Migrations\Migrator;
use Flow\PostgreSql\Migrations\Store\MigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationRepository;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigrationGenerator;
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\Migrations\VersionResolver;
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;

use function sys_get_temp_dir;

final class MigrationsFactoryTest extends TestCase
{
    public function test_create_diff_generator(): void
    {
        $factory = new MigrationsFactory(
            new Configuration(
                new SpyClient(),
                new FakeCatalogProvider(new Catalog([])),
                sys_get_temp_dir() . '/flow_migrations_test',
                'App\\Migrations',
            ),
            new FakeMigrationRepository(),
        );

        static::assertInstanceOf(
            DiffMigrationGenerator::class,
            $factory->createDiffGenerator(new SpyMigrationGenerator(Version::fromString('20260401120000'))),
        );
    }

    public function test_create_executor(): void
    {
        $factory = new MigrationsFactory(
            new Configuration(
                new SpyClient(),
                new FakeCatalogProvider(new Catalog([])),
                sys_get_temp_dir() . '/flow_migrations_test',
                'App\\Migrations',
            ),
            new FakeMigrationRepository(),
        );

        static::assertInstanceOf(MigrationExecutor::class, $factory->createExecutor());
    }

    public function test_create_migrator(): void
    {
        $factory = new MigrationsFactory(
            new Configuration(
                new SpyClient(),
                new FakeCatalogProvider(new Catalog([])),
                sys_get_temp_dir() . '/flow_migrations_test',
                'App\\Migrations',
            ),
            new FakeMigrationRepository(),
        );

        static::assertInstanceOf(Migrator::class, $factory->createMigrator());
    }

    public function test_create_store(): void
    {
        $factory = new MigrationsFactory(
            new Configuration(
                new SpyClient(),
                new FakeCatalogProvider(new Catalog([])),
                sys_get_temp_dir() . '/flow_migrations_test',
                'App\\Migrations',
            ),
            new FakeMigrationRepository(),
        );

        static::assertInstanceOf(MigrationStore::class, $factory->createStore());
    }

    public function test_create_version_resolver(): void
    {
        $factory = new MigrationsFactory(
            new Configuration(
                new SpyClient(),
                new FakeCatalogProvider(new Catalog([])),
                sys_get_temp_dir() . '/flow_migrations_test',
                'App\\Migrations',
            ),
            new FakeMigrationRepository(),
        );

        static::assertInstanceOf(VersionResolver::class, $factory->createVersionResolver());
    }
}
