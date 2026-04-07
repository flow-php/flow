<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit\Generator;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Generator\DiffMigrationGenerator;
use Flow\PostgreSql\Migrations\Tests\Double\{FakeCatalogProvider, SpyMigrationGenerator};
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\{Catalog, Column, Schema, Table};
use Flow\PostgreSql\Schema\Diff\CatalogComparator;
use PHPUnit\Framework\TestCase;

final class DiffMigrationGeneratorTest extends TestCase
{
    public function test_does_not_generate_rollback_when_disabled() : void
    {
        $source = new Catalog([]);
        $target = new Catalog([
            new Schema('public', [
                new Table('public', 'users', [
                    new Column('id', ColumnType::integer(), false),
                ]),
            ]),
        ]);

        $version = Version::fromString('20260403120000');
        $spy = new SpyMigrationGenerator($version);

        $diffGenerator = new DiffMigrationGenerator(
            new FakeCatalogProvider($source),
            new FakeCatalogProvider($target),
            CatalogComparator::create(),
            $spy,
            generateRollback: false,
        );

        $diffGenerator->generate('create_users');

        self::assertNotEmpty($spy->lastUpSql);
        self::assertNull($spy->lastDownSql);
    }

    public function test_does_not_throw_when_no_changes_and_allow_empty_is_true() : void
    {
        $catalog = new Catalog([]);
        $spy = new SpyMigrationGenerator(Version::fromString('20260403120000'));

        $diffGenerator = new DiffMigrationGenerator(
            new FakeCatalogProvider($catalog),
            new FakeCatalogProvider($catalog),
            CatalogComparator::create(),
            $spy,
        );

        $result = $diffGenerator->generate('empty_migration', allowEmpty: true);

        self::assertTrue($result->equals(Version::fromString('20260403120000')));
        self::assertSame([], $spy->lastUpSql);
        self::assertSame([], $spy->lastDownSql);
    }

    public function test_from_empty_schema_uses_empty_catalog_as_source() : void
    {
        $sourceCatalogWithData = new Catalog([
            new Schema('public', [
                new Table('public', 'existing_table', [
                    new Column('id', ColumnType::integer(), false),
                ]),
            ]),
        ]);

        $target = new Catalog([
            new Schema('public', [
                new Table('public', 'existing_table', [
                    new Column('id', ColumnType::integer(), false),
                ]),
                new Table('public', 'new_table', [
                    new Column('id', ColumnType::integer(), false),
                ]),
            ]),
        ]);

        $version = Version::fromString('20260403120000');
        $spy = new SpyMigrationGenerator($version);

        $diffGenerator = new DiffMigrationGenerator(
            new FakeCatalogProvider($sourceCatalogWithData),
            new FakeCatalogProvider($target),
            CatalogComparator::create(),
            $spy,
        );

        $diffGenerator->generate('initial_schema', fromEmptySchema: true);

        $upSqlJoined = \implode(' ', $spy->lastUpSql ?? []);
        self::assertStringContainsString('new_table', $upSqlJoined);
        self::assertStringContainsString('existing_table', $upSqlJoined);
    }

    public function test_generated_sql_content_comes_from_catalog_diff() : void
    {
        $source = new Catalog([]);
        $target = new Catalog([
            new Schema('public', [
                new Table('public', 'users', [
                    new Column('id', ColumnType::integer(), false),
                    new Column('name', ColumnType::text(), true),
                ]),
            ]),
        ]);

        $version = Version::fromString('20260403120000');
        $spy = new SpyMigrationGenerator($version);

        $diffGenerator = new DiffMigrationGenerator(
            new FakeCatalogProvider($source),
            new FakeCatalogProvider($target),
            CatalogComparator::create(),
            $spy,
        );

        $diffGenerator->generate('create_users');

        $upSqlJoined = \implode(' ', $spy->lastUpSql ?? []);
        $downSqlJoined = \implode(' ', $spy->lastDownSql ?? []);
        self::assertStringContainsString('CREATE', $upSqlJoined);
        self::assertStringContainsString('users', $upSqlJoined);
        self::assertStringContainsString('DROP', $downSqlJoined);
    }

    public function test_generates_migration_when_diff_has_changes() : void
    {
        $source = new Catalog([]);
        $target = new Catalog([
            new Schema('public', [
                new Table('public', 'users', [
                    new Column('id', ColumnType::integer(), false),
                ]),
            ]),
        ]);

        $version = Version::fromString('20260403120000');
        $spy = new SpyMigrationGenerator($version);

        $diffGenerator = new DiffMigrationGenerator(
            new FakeCatalogProvider($source),
            new FakeCatalogProvider($target),
            CatalogComparator::create(),
            $spy,
        );

        $result = $diffGenerator->generate('create_users');

        self::assertTrue($result->equals($version));
        self::assertSame('create_users', $spy->lastSchemaName);
        self::assertNotEmpty($spy->lastUpSql);
        self::assertNotEmpty($spy->lastDownSql);
    }

    public function test_throws_when_no_changes_detected_and_allow_empty_is_false() : void
    {
        $catalog = new Catalog([]);

        $diffGenerator = new DiffMigrationGenerator(
            new FakeCatalogProvider($catalog),
            new FakeCatalogProvider($catalog),
            CatalogComparator::create(),
            new SpyMigrationGenerator(Version::fromString('20260403120000')),
        );

        $this->expectException(MigrationException::class);
        $this->expectExceptionMessage('No changes detected');

        $diffGenerator->generate('empty_migration');
    }
}
