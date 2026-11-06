<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\Schema\Domain;
use Flow\PostgreSql\Schema\Func;
use Flow\PostgreSql\Schema\Index;
use Flow\PostgreSql\Schema\MaterializedView;
use Flow\PostgreSql\Schema\Procedure;
use Flow\PostgreSql\Schema\Sequence;
use Flow\PostgreSql\Schema\View;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function array_map;
use function extension_loaded;
use function Flow\PostgreSql\DSL\catalog;
use function Flow\PostgreSql\DSL\catalog_comparator;
use function Flow\PostgreSql\DSL\client_catalog_provider;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_text;
use function Flow\PostgreSql\DSL\schema_domain;
use function Flow\PostgreSql\DSL\schema_function;
use function Flow\PostgreSql\DSL\schema_index;
use function Flow\PostgreSql\DSL\schema_materialized_view;
use function Flow\PostgreSql\DSL\schema_procedure;
use function Flow\PostgreSql\DSL\schema_sequence;
use function Flow\PostgreSql\DSL\schema_table;
use function Flow\PostgreSql\DSL\schema_view;

final class NonPublicSchemaMigrationRoundTripTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_non_public_round_trip';

    protected function setUp(): void
    {
        parent::setUp();

        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA)->toSql());
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table('nprt_sync', self::SCHEMA)
                    ->column(column('id', column_type_integer()))
                    ->column(column('lease', column_type_text()))
                    ->toSql(),
            );
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->dropMaterializedViewIfExists('public.nprt_mv_a');
        $this->pgsqlContext()->dropViewIfExists('public.nprt_v_a');
        $this->pgsqlContext()->dropSequenceIfExists('public.nprt_seq_a');
        $this->pgsqlContext()->dropDomainIfExists('public.nprt_dom_a');
        $this->pgsqlContext()->dropFunctionIfExists('nprt_fn_a');
        $this->pgsqlContext()->dropProcedureIfExists('nprt_proc_a');

        parent::tearDown();
    }

    public function test_migration_creates_objects_in_their_schema_and_rollback_removes_them(): void
    {
        $client = $this->pgsqlContext()->client();
        $columns = [schema_column_integer('id'), schema_column_text('lease')];
        $target = catalog([schema(
            self::SCHEMA,
            tables: [schema_table(
                'nprt_sync',
                $columns,
                indexes: [schema_index('nprt_idx_lease', ['lease'])],
                schema: self::SCHEMA,
            )],
            sequences: [schema_sequence('nprt_seq_a', ownedByTable: 'nprt_sync', ownedByColumn: 'id')],
            views: [schema_view('nprt_v_a', 'SELECT id FROM ' . self::SCHEMA . '.nprt_sync')],
            materializedViews: [
                schema_materialized_view('nprt_mv_a', 'SELECT id FROM ' . self::SCHEMA . '.nprt_sync', [
                    schema_index('nprt_idx_mv_a', ['id']),
                ]),
            ],
            functions: [schema_function('nprt_fn_a', 'integer', definition: 'SELECT 1')],
            procedures: [schema_procedure('nprt_proc_a', definition: 'SELECT 1')],
            domains: [schema_domain('nprt_dom_a', column_type_text())],
        )]);

        foreach (catalog_comparator()
            ->compare(client_catalog_provider($client, [self::SCHEMA])->get(), $target)
            ->generate() as $sql) {
            $client->execute($sql->toSql());
        }

        $migrated = client_catalog_provider($client, [self::SCHEMA])->get();
        $schema = $migrated->get(self::SCHEMA);
        $public = client_catalog_provider($client, ['public'])->get()->get('public');

        static::assertSame(['nprt_v_a'], array_map(static fn(View $view): string => $view->name, $schema->views));
        static::assertSame(
            ['nprt_mv_a'],
            array_map(static fn(MaterializedView $mv): string => $mv->name, $schema->materializedViews),
        );
        static::assertSame(
            ['nprt_idx_mv_a'],
            array_map(static fn(Index $idx): string => $idx->name, $schema->materializedViews[0]->indexes),
        );
        static::assertSame(
            ['nprt_seq_a'],
            array_map(static fn(Sequence $seq): string => $seq->name, $schema->sequences),
        );
        static::assertSame(['nprt_fn_a'], array_map(static fn(Func $func): string => $func->name, $schema->functions));
        static::assertSame(
            ['nprt_proc_a'],
            array_map(static fn(Procedure $proc): string => $proc->name, $schema->procedures),
        );
        static::assertSame(
            ['nprt_dom_a'],
            array_map(static fn(Domain $domain): string => $domain->name, $schema->domains),
        );
        static::assertSame(
            ['nprt_idx_lease'],
            array_map(static fn(Index $idx): string => $idx->name, $schema->table('nprt_sync')->indexes),
        );
        static::assertNotContains('nprt_v_a', array_map(static fn(View $view): string => $view->name, $public->views));
        static::assertNotContains('nprt_mv_a', array_map(
            static fn(MaterializedView $mv): string => $mv->name,
            $public->materializedViews,
        ));
        static::assertNotContains('nprt_seq_a', array_map(
            static fn(Sequence $seq): string => $seq->name,
            $public->sequences,
        ));
        static::assertNotContains('nprt_fn_a', array_map(
            static fn(Func $func): string => $func->name,
            $public->functions,
        ));
        static::assertNotContains('nprt_proc_a', array_map(
            static fn(Procedure $proc): string => $proc->name,
            $public->procedures,
        ));
        static::assertNotContains('nprt_dom_a', array_map(
            static fn(Domain $domain): string => $domain->name,
            $public->domains,
        ));

        $tableOnly = catalog([schema(self::SCHEMA, tables: [schema_table(
            'nprt_sync',
            $columns,
            schema: self::SCHEMA,
        )])]);

        foreach (catalog_comparator()->compare($migrated, $tableOnly)->generate() as $sql) {
            $client->execute($sql->toSql());
        }

        $rolledBack = client_catalog_provider($client, [self::SCHEMA])->get()->get(self::SCHEMA);

        static::assertSame([], $rolledBack->views);
        static::assertSame([], $rolledBack->materializedViews);
        static::assertSame([], $rolledBack->sequences);
        static::assertSame([], $rolledBack->functions);
        static::assertSame([], $rolledBack->procedures);
        static::assertSame([], $rolledBack->domains);
        static::assertSame([], $rolledBack->table('nprt_sync')->indexes);
    }
}
