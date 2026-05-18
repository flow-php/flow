<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Generator;

use Flow\Bridge\Symfony\PostgreSqlBundle\Generator\TwigMigrationGenerator;
use Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Double\TemporaryDirectory;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\FixedVersionGenerator;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;
use Twig\Environment;
use Twig\Loader\FilesystemLoader;

use function dirname;
use function file_get_contents;

final class TwigMigrationGeneratorTest extends TestCase
{
    public function templatesPath(): string
    {
        return dirname(__DIR__, 8) . '/src/Flow/Bridge/Symfony/PostgreSqlBundle/Resources/templates';
    }

    public function test_generate_data_migration_creates_files(): void
    {
        $tmpDir = new TemporaryDirectory();

        try {
            $generator = new TwigMigrationGenerator(
                new Configuration(
                    new SpyClient(),
                    new FakeCatalogProvider(new Catalog([])),
                    $tmpDir->path,
                    'App\\Migrations',
                ),
                new FixedVersionGenerator(Version::fromString('20260401120000')),
                new Environment(new FilesystemLoader($this->templatesPath())),
                new NativeLocalFilesystem(),
            );

            $generator->generateDataMigration('test_migration');

            static::assertFileExists($tmpDir->path . '/20260401120000_test_migration/migration.php');
            static::assertFileExists($tmpDir->path . '/20260401120000_test_migration/rollback.php');
            static::assertStringContainsString(
                'App\\Migrations',
                (string) file_get_contents($tmpDir->path . '/20260401120000_test_migration/migration.php'),
            );
            static::assertStringContainsString(
                'App\\Migrations',
                (string) file_get_contents($tmpDir->path . '/20260401120000_test_migration/rollback.php'),
            );
        } finally {
            $tmpDir->cleanUp();
        }
    }

    public function test_generate_data_migration_without_rollback_when_disabled(): void
    {
        $tmpDir = new TemporaryDirectory();

        try {
            $generator = new TwigMigrationGenerator(
                new Configuration(
                    new SpyClient(),
                    new FakeCatalogProvider(new Catalog([])),
                    $tmpDir->path,
                    'App\\Migrations',
                    generateRollback: false,
                ),
                new FixedVersionGenerator(Version::fromString('20260401120000')),
                new Environment(new FilesystemLoader($this->templatesPath())),
                new NativeLocalFilesystem(),
            );

            $generator->generateDataMigration('test_migration');

            static::assertFileExists($tmpDir->path . '/20260401120000_test_migration/migration.php');
            static::assertFileDoesNotExist($tmpDir->path . '/20260401120000_test_migration/rollback.php');
        } finally {
            $tmpDir->cleanUp();
        }
    }

    public function test_generate_schema_migration_creates_files(): void
    {
        $tmpDir = new TemporaryDirectory();

        try {
            $generator = new TwigMigrationGenerator(
                new Configuration(
                    new SpyClient(),
                    new FakeCatalogProvider(new Catalog([])),
                    $tmpDir->path,
                    'App\\Migrations',
                ),
                new FixedVersionGenerator(Version::fromString('20260401120000')),
                new Environment(new FilesystemLoader($this->templatesPath())),
                new NativeLocalFilesystem(),
            );

            $generator->generateSchemaMigration('add_table', ['CREATE TABLE test (id INT)'], ['DROP TABLE test']);

            static::assertFileExists($tmpDir->path . '/20260401120000_add_table/migration.php');
            static::assertFileExists($tmpDir->path . '/20260401120000_add_table/rollback.php');

            $migrationContent = (string) file_get_contents($tmpDir->path . '/20260401120000_add_table/migration.php');
            static::assertStringContainsString('CREATE TABLE test (id INT)', $migrationContent);

            $rollbackContent = (string) file_get_contents($tmpDir->path . '/20260401120000_add_table/rollback.php');
            static::assertStringContainsString('DROP TABLE test', $rollbackContent);
        } finally {
            $tmpDir->cleanUp();
        }
    }

    public function test_generate_schema_migration_without_rollback(): void
    {
        $tmpDir = new TemporaryDirectory();

        try {
            $generator = new TwigMigrationGenerator(
                new Configuration(
                    new SpyClient(),
                    new FakeCatalogProvider(new Catalog([])),
                    $tmpDir->path,
                    'App\\Migrations',
                ),
                new FixedVersionGenerator(Version::fromString('20260401120000')),
                new Environment(new FilesystemLoader($this->templatesPath())),
                new NativeLocalFilesystem(),
            );

            $generator->generateSchemaMigration('add_table', ['CREATE TABLE test (id INT)']);

            static::assertFileExists($tmpDir->path . '/20260401120000_add_table/migration.php');
            static::assertFileDoesNotExist($tmpDir->path . '/20260401120000_add_table/rollback.php');
        } finally {
            $tmpDir->cleanUp();
        }
    }

    public function test_returns_version_from_generator(): void
    {
        $tmpDir = new TemporaryDirectory();

        try {
            $generator = new TwigMigrationGenerator(
                new Configuration(
                    new SpyClient(),
                    new FakeCatalogProvider(new Catalog([])),
                    $tmpDir->path,
                    'App\\Migrations',
                ),
                new FixedVersionGenerator(Version::fromString('20260401120000')),
                new Environment(new FilesystemLoader($this->templatesPath())),
                new NativeLocalFilesystem(),
            );

            static::assertTrue(Version::fromString('20260401120000')->equals($generator->generateDataMigration(
                'test',
            )));
        } finally {
            $tmpDir->cleanUp();
        }
    }
}
