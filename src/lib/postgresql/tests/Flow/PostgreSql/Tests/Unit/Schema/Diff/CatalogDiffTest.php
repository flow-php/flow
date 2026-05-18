<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\Diff\CatalogDiff;
use Flow\PostgreSql\Schema\Diff\SchemaDiff;
use PHPUnit\Framework\TestCase;

use function count;
use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_text;
use function Flow\PostgreSql\DSL\schema_sequence;
use function Flow\PostgreSql\DSL\schema_table;
use function str_contains;

final class CatalogDiffTest extends TestCase
{
    public function test_creates_added_schema(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([schema('public'), schema('audit')]);

        $diff = new CatalogDiff($source, $target, addedSchemas: [schema('audit')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('CREATE SCHEMA IF NOT EXISTS audit', $sqls[0]->toSql());
    }

    public function test_creates_added_schema_with_objects(): void
    {
        $source = new Catalog([schema('public')]);

        $auditSchema = schema(
            'audit',
            tables: [schema_table(
                'events',
                [schema_column_integer('id', false), schema_column_text('name')],
                schema: 'audit',
            )],
            sequences: [schema_sequence('events_id_seq')],
        );

        $target = new Catalog([schema('public'), $auditSchema]);

        $diff = new CatalogDiff($source, $target, addedSchemas: [$auditSchema]);

        $sqls = $diff->generate();

        static::assertGreaterThanOrEqual(3, count($sqls));
        static::assertSame('CREATE SCHEMA IF NOT EXISTS audit', $sqls[0]->toSql());
        static::assertStringContainsString('CREATE SEQUENCE', $sqls[1]->toSql());
        static::assertStringContainsString('events_id_seq', $sqls[1]->toSql());
        static::assertStringContainsString('CREATE TABLE', $sqls[2]->toSql());
        static::assertStringContainsString('events', $sqls[2]->toSql());
    }

    public function test_delegates_to_schema_diff(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([schema('public')]);

        $table = schema_table('users', [schema_column_integer('id', false)]);

        $schemaDiff = new SchemaDiff(schema('public'), schema('public', tables: [$table]), addedTables: [$table]);

        $diff = new CatalogDiff($source, $target, modifiedSchemas: [$schemaDiff]);

        $sqls = $diff->generate();

        static::assertNotEmpty($sqls);
        static::assertStringContainsString('CREATE TABLE', $sqls[0]->toSql());
        static::assertStringContainsString('users', $sqls[0]->toSql());
    }

    public function test_drops_removed_schema(): void
    {
        $source = new Catalog([schema('public'), schema('audit')]);
        $target = new Catalog([schema('public')]);

        $diff = new CatalogDiff($source, $target, removedSchemas: [schema('audit')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP SCHEMA audit CASCADE', $sqls[0]->toSql());
    }

    public function test_empty_diff(): void
    {
        $catalog = new Catalog([schema('public')]);

        $diff = new CatalogDiff($catalog, $catalog);

        static::assertTrue($diff->isEmpty());
    }

    public function test_handles_multiple_schemas(): void
    {
        $source = new Catalog([schema('public'), schema('old_schema')]);
        $target = new Catalog([schema('public'), schema('new_schema')]);

        $diff = new CatalogDiff(
            $source,
            $target,
            addedSchemas: [schema('new_schema')],
            removedSchemas: [schema('old_schema')],
        );

        $sqls = $diff->generate();

        static::assertCount(2, $sqls);
        static::assertSame('CREATE SCHEMA IF NOT EXISTS new_schema', $sqls[0]->toSql());
        static::assertSame('DROP SCHEMA old_schema CASCADE', $sqls[1]->toSql());
    }

    public function test_not_empty_when_schema_added(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([schema('public'), schema('audit')]);

        $diff = new CatalogDiff($source, $target, addedSchemas: [schema('audit')]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_not_empty_when_schema_modified(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([schema('public')]);

        $schemaDiff = new SchemaDiff(
            schema('public'),
            schema('public', tables: [schema_table('users', [schema_column_integer('id', false)])]),
            addedTables: [schema_table('users', [schema_column_integer('id', false)])],
        );

        $diff = new CatalogDiff($source, $target, modifiedSchemas: [$schemaDiff]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_not_empty_when_schema_removed(): void
    {
        $source = new Catalog([schema('public'), schema('audit')]);
        $target = new Catalog([schema('public')]);

        $diff = new CatalogDiff($source, $target, removedSchemas: [schema('audit')]);

        static::assertFalse($diff->isEmpty());
    }

    public function test_ordering_added_before_modified_before_removed(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $schemaDiff = new SchemaDiff(schema('existing'), schema('existing', tables: [$table]), addedTables: [$table]);

        $diff = new CatalogDiff(
            new Catalog([schema('public'), schema('existing'), schema('old')]),
            new Catalog([schema('new'), schema('existing'), schema('public')]),
            addedSchemas: [schema('new')],
            removedSchemas: [schema('old')],
            modifiedSchemas: [$schemaDiff],
        );

        $sqls = $diff->generate();

        static::assertSame('CREATE SCHEMA IF NOT EXISTS new', $sqls[0]->toSql());

        $createTableIdx = null;
        $dropSchemaIdx = null;

        foreach ($sqls as $i => $sql) {
            if (str_contains($sql->toSql(), 'CREATE TABLE') && $createTableIdx === null) {
                $createTableIdx = $i;
            }

            if (str_contains($sql->toSql(), 'DROP SCHEMA old CASCADE') && $dropSchemaIdx === null) {
                $dropSchemaIdx = $i;
            }
        }

        static::assertNotNull($createTableIdx);
        static::assertNotNull($dropSchemaIdx);
        static::assertGreaterThan($createTableIdx, $dropSchemaIdx);
    }

    public function test_returns_empty_when_no_changes(): void
    {
        $catalog = new Catalog([]);

        $diff = new CatalogDiff($catalog, $catalog);

        static::assertSame([], $diff->generate());
    }

    public function test_reversed_added_schema(): void
    {
        $source = new Catalog([schema('public'), schema('audit')]);
        $target = new Catalog([schema('public')]);

        $diff = new CatalogDiff($source, $target, removedSchemas: [schema('audit')]);

        $sqls = $diff->generate();

        static::assertCount(1, $sqls);
        static::assertSame('DROP SCHEMA audit CASCADE', $sqls[0]->toSql());
    }

    public function test_reversed_creates_removed_schema(): void
    {
        $auditSchema = schema('audit', tables: [schema_table(
            'events',
            [schema_column_integer('id', false)],
            schema: 'audit',
        )]);

        $source = new Catalog([schema('public')]);
        $target = new Catalog([schema('public'), $auditSchema]);

        $diff = new CatalogDiff($source, $target, addedSchemas: [$auditSchema]);

        $sqls = $diff->generate();

        static::assertGreaterThanOrEqual(2, count($sqls));
        static::assertSame('CREATE SCHEMA IF NOT EXISTS audit', $sqls[0]->toSql());
        static::assertStringContainsString('CREATE TABLE', $sqls[1]->toSql());
        static::assertStringContainsString('events', $sqls[1]->toSql());
    }

    public function test_reversed_creates_removed_schema_with_sequence(): void
    {
        $auditSchema = schema(
            'audit',
            tables: [schema_table('events', [schema_column_integer('id', false)], schema: 'audit')],
            sequences: [schema_sequence('events_id_seq')],
        );

        $source = new Catalog([schema('public')]);
        $target = new Catalog([schema('public'), $auditSchema]);

        $diff = new CatalogDiff($source, $target, addedSchemas: [$auditSchema]);

        $sqls = $diff->generate();

        static::assertSame('CREATE SCHEMA IF NOT EXISTS audit', $sqls[0]->toSql());
        static::assertStringContainsString('CREATE SEQUENCE', $sqls[1]->toSql());
        static::assertStringContainsString('events_id_seq', $sqls[1]->toSql());
        static::assertStringContainsString('CREATE TABLE', $sqls[2]->toSql());
    }

    public function test_reversed_delegates_to_schema_diff(): void
    {
        $source = new Catalog([schema('public')]);
        $target = new Catalog([schema('public')]);

        $table = schema_table('users', [schema_column_integer('id', false)]);

        $schemaDiff = new SchemaDiff(schema('public', tables: [$table]), schema('public'), removedTables: [$table]);

        $diff = new CatalogDiff($target, $source, modifiedSchemas: [$schemaDiff]);

        $sqls = $diff->generate();

        static::assertNotEmpty($sqls);
        static::assertStringContainsString('DROP TABLE', $sqls[0]->toSql());
    }

    public function test_reversed_ordering_added_before_modified_before_removed(): void
    {
        $table = schema_table('users', [schema_column_integer('id', false)]);

        $schemaDiff = new SchemaDiff(schema('existing', tables: [$table]), schema('existing'), removedTables: [$table]);

        $diff = new CatalogDiff(
            new Catalog([schema('new'), schema('existing'), schema('public')]),
            new Catalog([schema('public'), schema('existing'), schema('old')]),
            addedSchemas: [schema('old')],
            removedSchemas: [schema('new')],
            modifiedSchemas: [$schemaDiff],
        );

        $sqls = $diff->generate();

        static::assertSame('CREATE SCHEMA IF NOT EXISTS old', $sqls[0]->toSql());

        $dropTableIdx = null;
        $dropSchemaIdx = null;

        foreach ($sqls as $i => $sql) {
            if (str_contains($sql->toSql(), 'DROP TABLE') && $dropTableIdx === null) {
                $dropTableIdx = $i;
            }

            if (str_contains($sql->toSql(), 'DROP SCHEMA new CASCADE') && $dropSchemaIdx === null) {
                $dropSchemaIdx = $i;
            }
        }

        static::assertNotNull($dropTableIdx);
        static::assertNotNull($dropSchemaIdx);
        static::assertGreaterThan($dropTableIdx, $dropSchemaIdx);
    }
}
