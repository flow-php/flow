<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Schema\Diff;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\Column;
use Flow\PostgreSql\Schema\Diff\CatalogDiff;
use Flow\PostgreSql\Schema\Diff\ColumnDiff;
use Flow\PostgreSql\Schema\Diff\SchemaDiff;
use Flow\PostgreSql\Schema\Diff\TableDiff;
use PHPUnit\Framework\TestCase;

use function array_map;
use function Flow\PostgreSql\DSL\ast_view_dependency_resolver;
use function Flow\PostgreSql\DSL\noop_view_dependency_resolver;
use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_column_bigint;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_text;
use function Flow\PostgreSql\DSL\schema_materialized_view;
use function Flow\PostgreSql\DSL\schema_table;
use function Flow\PostgreSql\DSL\schema_view;

final class CatalogDiffViewRebuildTest extends TestCase
{
    public function test_cascading_view_dependencies_resolved(): void
    {
        $sourceTable = schema_table(
            'events',
            [schema_column_integer('id', false), schema_column_integer('amount')],
            schema: 'data',
        );
        $targetTable = schema_table(
            'events',
            [schema_column_integer('id', false), schema_column_bigint('amount')],
            schema: 'data',
        );

        $source = new Catalog([
            schema('data', tables: [$sourceTable]),
            schema('report', views: [schema_view('event_summary', 'SELECT * FROM data.events')]),
            schema('public', views: [schema_view('dashboard', 'SELECT * FROM report.event_summary')]),
        ]);

        $target = new Catalog([
            schema('data', tables: [$targetTable]),
            schema('report', views: [schema_view('event_summary', 'SELECT * FROM data.events')]),
            schema('public', views: [schema_view('dashboard', 'SELECT * FROM report.event_summary')]),
        ]);

        $diff = new CatalogDiff(
            $source,
            $target,
            modifiedSchemas: [
                new SchemaDiff($source->get('data'), $target->get('data'), modifiedTables: [
                    new TableDiff($sourceTable, $targetTable, modifiedColumns: [new ColumnDiff(
                        'data.events',
                        schema_column_integer('amount'),
                        schema_column_bigint('amount'),
                    )]),
                ]),
            ],
            viewDependencyResolver: ast_view_dependency_resolver(),
        );

        $sqls = array_map(static fn($q) => $q->toSql(), $diff->generate());

        static::assertSame('DROP VIEW public.dashboard', $sqls[0]);
        static::assertSame('DROP VIEW report.event_summary', $sqls[1]);
        static::assertStringContainsString('ALTER TABLE data.events ALTER COLUMN amount TYPE bigint', $sqls[2]);
        static::assertSame('CREATE VIEW report.event_summary AS SELECT * FROM data.events', $sqls[3]);
        static::assertSame('CREATE VIEW public.dashboard AS SELECT * FROM report.event_summary', $sqls[4]);
    }

    public function test_materialized_view_dropped_and_recreated_when_dependent_table_changes(): void
    {
        $sourceTable = schema_table('orders', [schema_column_integer('id', false), schema_column_integer('total')]);
        $targetTable = schema_table('orders', [schema_column_integer('id', false), schema_column_bigint('total')]);

        $source = new Catalog([
            schema(
                'public',
                tables: [$sourceTable],
                materializedViews: [schema_materialized_view('mv_order_stats', 'SELECT * FROM orders')],
            ),
        ]);

        $target = new Catalog([
            schema(
                'public',
                tables: [$targetTable],
                materializedViews: [schema_materialized_view('mv_order_stats', 'SELECT * FROM orders')],
            ),
        ]);

        $diff = new CatalogDiff(
            $source,
            $target,
            modifiedSchemas: [
                new SchemaDiff($source->get('public'), $target->get('public'), modifiedTables: [
                    new TableDiff($sourceTable, $targetTable, modifiedColumns: [new ColumnDiff(
                        'public.orders',
                        schema_column_integer('total'),
                        schema_column_bigint('total'),
                    )]),
                ]),
            ],
            viewDependencyResolver: ast_view_dependency_resolver(),
        );

        $sqls = array_map(static fn($q) => $q->toSql(), $diff->generate());

        static::assertSame('DROP MATERIALIZED VIEW public.mv_order_stats', $sqls[0]);
        static::assertStringContainsString('ALTER TABLE public.orders ALTER COLUMN total TYPE bigint', $sqls[1]);
        static::assertSame('CREATE MATERIALIZED VIEW public.mv_order_stats AS SELECT * FROM orders', $sqls[2]);
    }

    public function test_noop_resolver_skips_view_rebuild_even_when_table_column_type_changes(): void
    {
        $sourceTable = schema_table('data', [schema_column_integer('id', false), schema_column_integer('amount')]);
        $targetTable = schema_table('data', [schema_column_integer('id', false), schema_column_bigint('amount')]);

        $source = new Catalog([
            schema('public', tables: [$sourceTable], views: [schema_view('all_data', 'SELECT * FROM data')]),
        ]);

        $target = new Catalog([
            schema('public', tables: [$targetTable], views: [schema_view('all_data', 'SELECT * FROM data')]),
        ]);

        $diff = new CatalogDiff(
            $source,
            $target,
            modifiedSchemas: [
                new SchemaDiff($source->get('public'), $target->get('public'), modifiedTables: [
                    new TableDiff($sourceTable, $targetTable, modifiedColumns: [new ColumnDiff(
                        'public.data',
                        schema_column_integer('amount'),
                        schema_column_bigint('amount'),
                    )]),
                ]),
            ],
            viewDependencyResolver: noop_view_dependency_resolver(),
        );

        $sqls = array_map(static fn($q) => $q->toSql(), $diff->generate());

        static::assertCount(1, $sqls);
        static::assertStringContainsString('ALTER TABLE public.data ALTER COLUMN amount TYPE bigint', $sqls[0]);
    }

    public function test_view_dropped_and_recreated_when_dependent_table_column_type_changes(): void
    {
        $sourceTable = schema_table(
            'full_data',
            [schema_column_integer('id', false), schema_column_integer('amount')],
            schema: 'enriched',
        );
        $targetTable = schema_table(
            'full_data',
            [schema_column_integer('id', false), schema_column_bigint('amount')],
            schema: 'enriched',
        );

        $source = new Catalog([
            schema('enriched', tables: [$sourceTable]),
            schema('public', views: [schema_view('enriched_full_data', 'SELECT * FROM enriched.full_data')]),
        ]);

        $target = new Catalog([
            schema('enriched', tables: [$targetTable]),
            schema('public', views: [schema_view('enriched_full_data', 'SELECT * FROM enriched.full_data')]),
        ]);

        $diff = new CatalogDiff(
            $source,
            $target,
            modifiedSchemas: [
                new SchemaDiff($source->get('enriched'), $target->get('enriched'), modifiedTables: [
                    new TableDiff($sourceTable, $targetTable, modifiedColumns: [new ColumnDiff(
                        'enriched.full_data',
                        schema_column_integer('amount'),
                        schema_column_bigint('amount'),
                    )]),
                ]),
            ],
            viewDependencyResolver: ast_view_dependency_resolver(),
        );

        $sqls = array_map(static fn($q) => $q->toSql(), $diff->generate());

        static::assertSame('DROP VIEW public.enriched_full_data', $sqls[0]);
        static::assertStringContainsString('ALTER TABLE enriched.full_data ALTER COLUMN amount TYPE bigint', $sqls[1]);
        static::assertSame('CREATE VIEW public.enriched_full_data AS SELECT * FROM enriched.full_data', $sqls[2]);
    }

    public function test_view_dropped_with_if_exists_when_flag_enabled(): void
    {
        $sourceTable = schema_table(
            'full_data',
            [schema_column_integer('id', false), schema_column_integer('amount')],
            schema: 'enriched',
        );
        $targetTable = schema_table(
            'full_data',
            [schema_column_integer('id', false), schema_column_bigint('amount')],
            schema: 'enriched',
        );

        $source = new Catalog([
            schema('enriched', tables: [$sourceTable]),
            schema('public', views: [schema_view('enriched_full_data', 'SELECT * FROM enriched.full_data')]),
        ]);

        $target = new Catalog([
            schema('enriched', tables: [$targetTable]),
            schema('public', views: [schema_view('enriched_full_data', 'SELECT * FROM enriched.full_data')]),
        ]);

        $diff = new CatalogDiff(
            $source,
            $target,
            modifiedSchemas: [
                new SchemaDiff($source->get('enriched'), $target->get('enriched'), modifiedTables: [
                    new TableDiff($sourceTable, $targetTable, modifiedColumns: [new ColumnDiff(
                        'enriched.full_data',
                        schema_column_integer('amount'),
                        schema_column_bigint('amount'),
                    )]),
                ]),
            ],
            viewDependencyResolver: ast_view_dependency_resolver(),
            dropIfExists: true,
        );

        $sqls = array_map(static fn($q) => $q->toSql(), $diff->generate());

        static::assertSame('DROP VIEW IF EXISTS public.enriched_full_data', $sqls[0]);
        static::assertStringContainsString('ALTER TABLE enriched.full_data ALTER COLUMN amount TYPE bigint', $sqls[1]);
        static::assertSame('CREATE VIEW public.enriched_full_data AS SELECT * FROM enriched.full_data', $sqls[2]);
    }

    public function test_materialized_view_dropped_with_if_exists_when_flag_enabled(): void
    {
        $sourceTable = schema_table('orders', [schema_column_integer('id', false), schema_column_integer('total')]);
        $targetTable = schema_table('orders', [schema_column_integer('id', false), schema_column_bigint('total')]);

        $source = new Catalog([
            schema(
                'public',
                tables: [$sourceTable],
                materializedViews: [schema_materialized_view('mv_order_stats', 'SELECT * FROM orders')],
            ),
        ]);

        $target = new Catalog([
            schema(
                'public',
                tables: [$targetTable],
                materializedViews: [schema_materialized_view('mv_order_stats', 'SELECT * FROM orders')],
            ),
        ]);

        $diff = new CatalogDiff(
            $source,
            $target,
            modifiedSchemas: [
                new SchemaDiff($source->get('public'), $target->get('public'), modifiedTables: [
                    new TableDiff($sourceTable, $targetTable, modifiedColumns: [new ColumnDiff(
                        'public.orders',
                        schema_column_integer('total'),
                        schema_column_bigint('total'),
                    )]),
                ]),
            ],
            viewDependencyResolver: ast_view_dependency_resolver(),
            dropIfExists: true,
        );

        $sqls = array_map(static fn($q) => $q->toSql(), $diff->generate());

        static::assertSame('DROP MATERIALIZED VIEW IF EXISTS public.mv_order_stats', $sqls[0]);
        static::assertStringContainsString('ALTER TABLE public.orders ALTER COLUMN total TYPE bigint', $sqls[1]);
        static::assertSame('CREATE MATERIALIZED VIEW public.mv_order_stats AS SELECT * FROM orders', $sqls[2]);
    }

    public function test_view_not_affected_when_unrelated_table_modified(): void
    {
        $sourceTable = schema_table('orders', [schema_column_integer('id', false), schema_column_integer('amount')]);
        $targetTable = schema_table('orders', [schema_column_integer('id', false), schema_column_bigint('amount')]);

        $source = new Catalog([
            schema(
                'public',
                tables: [$sourceTable, schema_table('users', [schema_column_integer('id', false)])],
                views: [schema_view('active_users', 'SELECT * FROM users')],
            ),
        ]);

        $target = new Catalog([
            schema(
                'public',
                tables: [$targetTable, schema_table('users', [schema_column_integer('id', false)])],
                views: [schema_view('active_users', 'SELECT * FROM users')],
            ),
        ]);

        $diff = new CatalogDiff(
            $source,
            $target,
            modifiedSchemas: [
                new SchemaDiff($source->get('public'), $target->get('public'), modifiedTables: [
                    new TableDiff($sourceTable, $targetTable, modifiedColumns: [new ColumnDiff(
                        'public.orders',
                        schema_column_integer('amount'),
                        schema_column_bigint('amount'),
                    )]),
                ]),
            ],
            viewDependencyResolver: ast_view_dependency_resolver(),
        );

        $sqls = array_map(static fn($q) => $q->toSql(), $diff->generate());

        foreach ($sqls as $sql) {
            static::assertStringNotContainsString('DROP VIEW', $sql);
            static::assertStringNotContainsString('CREATE VIEW', $sql);
        }
    }

    public function test_view_not_dropped_when_table_only_adds_column(): void
    {
        $sourceTable = schema_table('users', [schema_column_integer('id', false)]);
        $targetTable = schema_table('users', [schema_column_integer('id', false), schema_column_text('name')]);

        $source = new Catalog([
            schema('public', tables: [$sourceTable], views: [schema_view('all_users', 'SELECT * FROM users')]),
        ]);

        $target = new Catalog([
            schema('public', tables: [$targetTable], views: [schema_view('all_users', 'SELECT * FROM users')]),
        ]);

        $diff = new CatalogDiff(
            $source,
            $target,
            modifiedSchemas: [
                new SchemaDiff($source->get('public'), $target->get('public'), modifiedTables: [
                    new TableDiff($sourceTable, $targetTable, addedColumns: [new Column(
                        'name',
                        ColumnType::text(),
                        true,
                    )]),
                ]),
            ],
            viewDependencyResolver: ast_view_dependency_resolver(),
        );

        $sqls = array_map(static fn($q) => $q->toSql(), $diff->generate());

        foreach ($sqls as $sql) {
            static::assertStringNotContainsString('DROP VIEW', $sql);
        }

        static::assertCount(1, $sqls);
        static::assertSame('ALTER TABLE public.users ADD COLUMN name pg_catalog.text', $sqls[0]);
    }
}
