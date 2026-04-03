<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Repository;

use function Flow\Filesystem\DSL\{native_local_filesystem, path};

use Flow\Bridge\Symfony\PostgreSqlBundle\Repository\FilesystemMigrationRepository;
use Flow\PostgreSql\Migrations\{Configuration, Migration, Rollback, Version};
use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Tests\Double\{FakeCatalogProvider, SpyClient, TemporaryDirectory};
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;

final class FilesystemMigrationRepositoryTest extends TestCase
{
    public function fixturesPath() : string
    {
        return __DIR__ . '/../../Fixture/migrations';
    }

    public function test_all_migrations_are_loaded_as_migration_instances() : void
    {
        $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($this->fixturesPath()), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $this->fixturesPath(), 'Test'));

        foreach ($repository->all() as $migration) {
            self::assertInstanceOf(Migration::class, $migration->migration);
        }
    }

    public function test_discovers_all_migrations_from_directory() : void
    {
        $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($this->fixturesPath()), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $this->fixturesPath(), 'Test'));

        self::assertCount(3, $repository->all());
    }

    public function test_get_returns_migration_for_existing_version() : void
    {
        $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($this->fixturesPath()), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $this->fixturesPath(), 'Test'));

        $migration = $repository->get(Version::fromString('20260401120000'));

        self::assertTrue(Version::fromString('20260401120000')->equals($migration->version));
    }

    public function test_get_throws_on_missing_version() : void
    {
        $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($this->fixturesPath()), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $this->fixturesPath(), 'Test'));

        $this->expectException(MigrationException::class);
        $repository->get(Version::fromString('99999999999999'));
    }

    public function test_has_returns_false_for_missing_version() : void
    {
        $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($this->fixturesPath()), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $this->fixturesPath(), 'Test'));

        self::assertFalse($repository->has(Version::fromString('99999999999999')));
    }

    public function test_has_returns_true_for_existing_version() : void
    {
        $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($this->fixturesPath()), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $this->fixturesPath(), 'Test'));

        self::assertTrue($repository->has(Version::fromString('20260401120000')));
    }

    public function test_migrations_are_sorted_by_version() : void
    {
        $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($this->fixturesPath()), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $this->fixturesPath(), 'Test'));

        $items = \iterator_to_array($repository->all());

        self::assertTrue(Version::fromString('20260401120000')->equals($items[0]->version));
        self::assertTrue(Version::fromString('20260402100000')->equals($items[1]->version));
        self::assertTrue(Version::fromString('20260403090000')->equals($items[2]->version));
    }

    public function test_names_are_parsed_from_entry_names() : void
    {
        $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($this->fixturesPath()), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $this->fixturesPath(), 'Test'));

        self::assertSame('create_users', $repository->get(Version::fromString('20260401120000'))->name);
        self::assertSame('seed_data', $repository->get(Version::fromString('20260402100000'))->name);
        self::assertSame('add_column', $repository->get(Version::fromString('20260403090000'))->name);
    }

    public function test_rollback_is_loaded_when_present() : void
    {
        $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($this->fixturesPath()), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $this->fixturesPath(), 'Test'));

        self::assertInstanceOf(Rollback::class, $repository->get(Version::fromString('20260401120000'))->rollback);
    }

    public function test_rollback_is_null_when_absent() : void
    {
        $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($this->fixturesPath()), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $this->fixturesPath(), 'Test'));

        self::assertNull($repository->get(Version::fromString('20260403090000'))->rollback);
    }

    public function test_throws_on_invalid_migration() : void
    {
        $tmpDir = new TemporaryDirectory();

        try {
            \mkdir($tmpDir->path . '/20260401120000_test', 0777, true);
            \file_put_contents($tmpDir->path . '/20260401120000_test/migration.php', '<?php return "not a migration";');

            $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($tmpDir->path), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $tmpDir->path, 'Test'));

            $this->expectException(MigrationException::class);
            $repository->all();
        } finally {
            $tmpDir->cleanUp();
        }
    }

    public function test_throws_on_invalid_rollback() : void
    {
        $tmpDir = new TemporaryDirectory();

        try {
            \mkdir($tmpDir->path . '/20260401120000_test', 0777, true);
            \file_put_contents($tmpDir->path . '/20260401120000_test/migration.php', '<?php return new class implements \Flow\PostgreSql\Migrations\Migration { public function migrate(\Flow\PostgreSql\Migrations\MigrationContext $context): void {} public function transactional(): bool { return true; } };');
            \file_put_contents($tmpDir->path . '/20260401120000_test/rollback.php', '<?php return "not a rollback";');

            $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($tmpDir->path), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $tmpDir->path, 'Test'));

            $this->expectException(MigrationException::class);
            $repository->all();
        } finally {
            $tmpDir->cleanUp();
        }
    }

    public function test_throws_on_missing_migration_file() : void
    {
        $tmpDir = new TemporaryDirectory();

        try {
            \mkdir($tmpDir->path . '/20260401120000_test', 0777, true);

            $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($tmpDir->path), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $tmpDir->path, 'Test'));

            $this->expectException(MigrationException::class);
            $repository->all();
        } finally {
            $tmpDir->cleanUp();
        }
    }

    public function test_versions_are_parsed_from_entry_names() : void
    {
        $repository = new FilesystemMigrationRepository(native_local_filesystem(), path($this->fixturesPath()), new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), $this->fixturesPath(), 'Test'));

        self::assertTrue($repository->has(Version::fromString('20260401120000')));
        self::assertTrue($repository->has(Version::fromString('20260402100000')));
        self::assertTrue($repository->has(Version::fromString('20260403090000')));
    }
}
