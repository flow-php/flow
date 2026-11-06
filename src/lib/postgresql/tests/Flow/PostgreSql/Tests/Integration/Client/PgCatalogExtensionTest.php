<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\MaterializedView;
use Flow\PostgreSql\Schema\Procedure;
use Flow\PostgreSql\Schema\Sequence;
use Flow\PostgreSql\Schema\Table;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function array_map;
use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\catalog;
use function Flow\PostgreSql\DSL\catalog_comparator;
use function Flow\PostgreSql\DSL\client_catalog_provider;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_extension;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class PgCatalogExtensionTest extends PostgreSqlTestCase
{
    private const EXTENSIONS = ['pgcrypto', 'pg_buffercache', 'earthdistance', 'cube'];

    private const OTHER = 'flow_ext_test_other';

    private const SCHEMA = 'flow_ext_test';

    protected function setUp(): void
    {
        parent::setUp();

        foreach (self::EXTENSIONS as $extension) {
            $this->pgsqlContext()->dropExtensionIfExists($extension);
        }

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->dropSchemaIfExists(self::OTHER);
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA)->toSql());
        $this->pgsqlContext()->client()->execute(create()->schema(self::OTHER)->toSql());
    }

    protected function tearDown(): void
    {
        foreach (self::EXTENSIONS as $extension) {
            $this->pgsqlContext()->dropExtensionIfExists($extension);
        }

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->dropSchemaIfExists(self::OTHER);

        parent::tearDown();
    }

    public function test_extension_member_domain_is_not_read(): void
    {
        $client = $this->pgsqlContext()->client();
        $client->execute(create()->extension('earthdistance')->schema(self::SCHEMA)->cascade()->toSql());

        static::assertSame([], client_catalog_provider($client, [self::SCHEMA])->get()->get(self::SCHEMA)->domains);
    }

    public function test_extension_member_functions_are_not_read(): void
    {
        $client = $this->pgsqlContext()->client();
        $client->execute(create()->extension('pgcrypto')->schema(self::SCHEMA)->toSql());

        static::assertSame([], client_catalog_provider($client, [self::SCHEMA])->get()->get(self::SCHEMA)->functions);
    }

    public function test_extension_member_table_is_not_read(): void
    {
        $client = $this->pgsqlContext()->client();
        $client->execute(create()->extension('pgcrypto')->schema(self::SCHEMA)->toSql());
        $client->execute(
            create()->table('member_t', self::SCHEMA)->column(column('id', column_type_integer()))->toSql(),
        );
        $client->execute(create()->table('own_t', self::SCHEMA)->column(column('id', column_type_integer()))->toSql());
        $client->execute(
            alter()
                ->extension('pgcrypto')
                ->addTable(self::SCHEMA . '.member_t')
                ->toSql(),
        );

        static::assertSame(
            ['own_t'],
            array_map(
                static fn(Table $table): string => $table->name,
                client_catalog_provider($client, [self::SCHEMA])->get()->get(self::SCHEMA)->tables,
            ),
        );
    }

    public function test_extension_member_views_are_not_read(): void
    {
        $client = $this->pgsqlContext()->client();
        $client->execute(create()->extension('pg_buffercache')->schema(self::SCHEMA)->toSql());

        static::assertSame([], client_catalog_provider($client, [self::SCHEMA])->get()->get(self::SCHEMA)->views);
    }

    public function test_extension_moved_between_schemas_converges(): void
    {
        $client = $this->pgsqlContext()->client();
        $client->execute(create()->extension('pgcrypto')->schema(self::SCHEMA)->toSql());
        $provider = client_catalog_provider($client, [self::SCHEMA, self::OTHER]);
        $target = catalog([schema(self::SCHEMA), schema(self::OTHER)], [schema_extension(
            'pgcrypto',
            schema: self::OTHER,
        )]);

        $migration = catalog_comparator()->compare($provider->get(), $target)->generate();

        static::assertSame(
            ['ALTER EXTENSION pgcrypto SET SCHEMA flow_ext_test_other'],
            array_map(static fn(Sql $sql): string => $sql->toSql(), $migration),
        );

        foreach ($migration as $sql) {
            $client->execute($sql->toSql());
        }

        static::assertSame(self::OTHER, $provider->get()->extensions()[0]->schema);
        static::assertSame([], catalog_comparator()->compare($provider->get(), $target)->generate());
    }

    public function test_extensions_are_read_into_the_catalog_with_their_schema(): void
    {
        $client = $this->pgsqlContext()->client();
        $client->execute(create()->extension('pgcrypto')->schema(self::SCHEMA)->toSql());

        $extensions = client_catalog_provider($client, [self::SCHEMA, self::OTHER])->get()->extensions();

        static::assertCount(1, $extensions);
        static::assertSame('pgcrypto', $extensions[0]->name);
        static::assertSame(self::SCHEMA, $extensions[0]->schema);
        static::assertNotNull($extensions[0]->version);
    }

    public function test_non_member_sequence_view_and_procedure_are_still_read_next_to_an_extension(): void
    {
        $client = $this->pgsqlContext()->client();
        $client->execute(create()->extension('pgcrypto')->schema(self::SCHEMA)->toSql());
        $client->execute(create()->sequence('own_seq', self::SCHEMA)->toSql());
        $client->execute(create()->table('src', self::SCHEMA)->column(column('id', column_type_integer()))->toSql());
        $client->execute(
            create()
                ->materializedView('own_mv', self::SCHEMA)
                ->as(select(col('id'))->from(table('src', self::SCHEMA)))
                ->toSql(),
        );
        $client->execute(
            create()
                ->procedure(self::SCHEMA . '.own_proc')
                ->language('sql')
                ->as('SELECT 1')
                ->toSql(),
        );

        $schema = client_catalog_provider($client, [self::SCHEMA])->get()->get(self::SCHEMA);

        static::assertSame(['own_seq'], array_map(static fn(Sequence $s): string => $s->name, $schema->sequences));
        static::assertSame(
            ['own_mv'],
            array_map(static fn(MaterializedView $mv): string => $mv->name, $schema->materializedViews),
        );
        static::assertSame(['own_proc'], array_map(static fn(Procedure $p): string => $p->name, $schema->procedures));
    }

    public function test_unchanged_catalog_with_extension_gives_empty_diff(): void
    {
        $client = $this->pgsqlContext()->client();
        $client->execute(create()->extension('pgcrypto')->schema(self::SCHEMA)->toSql());

        static::assertSame(
            [],
            catalog_comparator()
                ->compare(
                    client_catalog_provider($client, [self::SCHEMA, self::OTHER])->get(),
                    catalog([schema(self::SCHEMA), schema(self::OTHER)], [schema_extension(
                        'pgcrypto',
                        schema: self::SCHEMA,
                    )]),
                )
                ->generate(),
        );
    }
}
