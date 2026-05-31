<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\client_catalog_provider;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\column;
use function Flow\PostgreSql\DSL\column_type_serial;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\exclude_pattern;
use function Flow\PostgreSql\DSL\exclude_schema;
use function Flow\PostgreSql\DSL\exclude_scoped;
use function Flow\PostgreSql\DSL\exclude_starts_with;
use function Flow\PostgreSql\DSL\primary_key;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;

final class PgCatalogExclusionTest extends PostgreSqlTestCase
{
    private const SCHEMA = 'flow_exclusion_test';

    private const SECONDARY_SCHEMA = 'flow_exclusion_test_secondary';

    protected function setUp(): void
    {
        parent::setUp();

        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->dropSchemaIfExists(self::SECONDARY_SCHEMA);
        $this->pgsqlContext()->client()->execute(create()->schema(self::SCHEMA)->toSql());
    }

    protected function tearDown(): void
    {
        $this->pgsqlContext()->dropSchemaIfExists(self::SCHEMA);
        $this->pgsqlContext()->dropSchemaIfExists(self::SECONDARY_SCHEMA);

        parent::tearDown();
    }

    public function test_excludes_a_sequence_matching_a_pattern(): void
    {
        $s = self::SCHEMA;

        $this->pgsqlContext()->client()->execute(create()->sequence('cache_42', $s)->toSql());
        $this->pgsqlContext()->client()->execute(create()->sequence('invoice_seq', $s)->toSql());

        $schema = client_catalog_provider(
            $this->pgsqlContext()->client(),
            [$s],
            exclude_pattern('/^cache_\d+$/'),
        )->get()->get($s);

        static::assertFalse($schema->hasSequence('cache_42'));
        static::assertTrue($schema->hasSequence('invoice_seq'));
    }

    public function test_excludes_a_view_scoped_to_views_only(): void
    {
        $s = self::SCHEMA;

        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table('orders', $s)
                    ->column(column('id', column_type_serial()))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->view('orders_tmp', $s)
                    ->as(select(col('id'))->from(table('orders', $s)))
                    ->toSql(),
            );

        $schema = client_catalog_provider(
            $this->pgsqlContext()->client(),
            [$s],
            exclude_scoped(exclude_starts_with('orders'), SchemaObjectType::VIEW),
        )->get()->get($s);

        static::assertTrue($schema->hasTable('orders'));
        static::assertSame([], $schema->views);
    }

    public function test_excludes_an_entire_schema(): void
    {
        $this->pgsqlContext()->client()->execute(create()->schema(self::SECONDARY_SCHEMA)->toSql());
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table('kept', self::SCHEMA)
                    ->column(column('id', column_type_serial()))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );
        $this
            ->pgsqlContext()
            ->client()
            ->execute(
                create()
                    ->table('dropped', self::SECONDARY_SCHEMA)
                    ->column(column('id', column_type_serial()))
                    ->constraint(primary_key('id'))
                    ->toSql(),
            );

        $catalog = client_catalog_provider(
            $this->pgsqlContext()->client(),
            [self::SCHEMA, self::SECONDARY_SCHEMA],
            exclude_schema(self::SECONDARY_SCHEMA),
        )->get();

        static::assertSame([self::SCHEMA], $catalog->names());
    }

    public function test_excludes_tables_matching_a_prefix(): void
    {
        $s = self::SCHEMA;

        foreach (['users', 'user_upload_1', 'user_upload_2'] as $name) {
            $this
                ->pgsqlContext()
                ->client()
                ->execute(
                    create()
                        ->table($name, $s)
                        ->column(column('id', column_type_serial()))
                        ->column(column('label', column_type_text()))
                        ->constraint(primary_key('id'))
                        ->toSql(),
                );
        }

        $schema = client_catalog_provider(
            $this->pgsqlContext()->client(),
            [$s],
            exclude_starts_with('user_upload_'),
        )->get()->get($s);

        static::assertSame(['users'], $schema->tableNames());
    }
}
