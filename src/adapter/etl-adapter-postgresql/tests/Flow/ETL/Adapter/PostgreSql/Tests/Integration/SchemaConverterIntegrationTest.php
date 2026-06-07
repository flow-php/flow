<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Integration;

use Flow\ETL\Adapter\PostgreSql\PostgreSqlMetadata;
use Flow\ETL\Adapter\PostgreSql\Tests\IntegrationTestCase;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function Flow\ETL\Adapter\PostgreSql\pgsql_table_to_flow_schema;
use function Flow\ETL\Adapter\PostgreSql\to_pgsql_schema_table;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\PostgreSql\DSL\client_catalog_provider;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\schema_table_options;

final class SchemaConverterIntegrationTest extends IntegrationTestCase
{
    private string $tableName = 'flow_postgresql_schema_converter_test';

    protected function setUp(): void
    {
        parent::setUp();

        $this->client->execute(drop()->table($this->tableName)->ifExists()->cascade());
    }

    protected function tearDown(): void
    {
        $this->client->execute(drop()->table($this->tableName)->ifExists()->cascade());
    }

    public function test_creates_table_from_flow_schema_and_reads_it_back(): void
    {
        $table = to_pgsql_schema_table(
            schema(
                int_schema('id', metadata: PostgreSqlMetadata::primaryKey('pk_' . $this->tableName)),
                str_schema('name', metadata: PostgreSqlMetadata::length(120)),
                str_schema('email', metadata: PostgreSqlMetadata::indexUnique('uq_' . $this->tableName . '_email')),
                bool_schema('active', metadata: PostgreSqlMetadata::default(true)),
                json_schema('payload'),
            ),
            $this->tableName,
        );

        foreach ($table->toSql() as $sql) {
            $this->client->execute($sql);
        }

        $introspected = client_catalog_provider($this->client, ['public'])
            ->get()
            ->get('public')
            ->table($this->tableName);

        static::assertSame(['id', 'name', 'email', 'active', 'payload'], $introspected->columnNames());
        static::assertNotNull($introspected->primaryKey);
        static::assertSame(['id'], $introspected->primaryKey->columns);

        $flowSchema = pgsql_table_to_flow_schema($introspected);

        static::assertInstanceOf(IntegerType::class, $flowSchema->get('id')->type());
        static::assertInstanceOf(StringType::class, $flowSchema->get('name')->type());
        static::assertInstanceOf(BooleanType::class, $flowSchema->get('active')->type());
        static::assertInstanceOf(JsonType::class, $flowSchema->get('payload')->type());
        static::assertFalse($flowSchema->get('id')->isNullable());
    }

    public function test_creates_index_with_explicit_column_order(): void
    {
        $indexName = 'idx_' . $this->tableName . '_keyset';

        $table = to_pgsql_schema_table(
            schema(
                int_schema(
                    'id',
                    metadata: PostgreSqlMetadata::primaryKey('pk_' . $this->tableName)->merge(PostgreSqlMetadata::index(
                        $indexName,
                        2,
                    )),
                ),
                datetime_schema('created_at', metadata: PostgreSqlMetadata::index($indexName, 1)),
            ),
            $this->tableName,
        );

        foreach ($table->toSql() as $sql) {
            $this->client->execute($sql);
        }

        $introspected = client_catalog_provider($this->client, ['public'])
            ->get()
            ->get('public')
            ->table($this->tableName);

        $keyset = null;

        foreach ($introspected->indexes as $index) {
            if ($index->name === $indexName) {
                $keyset = $index;
            }
        }

        static::assertNotNull($keyset);
        static::assertSame(['created_at', 'id'], $keyset->columns);
    }

    public function test_creates_unlogged_table_from_options(): void
    {
        $table = to_pgsql_schema_table(
            schema(
                int_schema('id', metadata: PostgreSqlMetadata::primaryKey('pk_' . $this->tableName)),
                str_schema('payload'),
            ),
            $this->tableName,
            options: schema_table_options(unlogged: true),
        );

        foreach ($table->toSql() as $sql) {
            $this->client->execute($sql);
        }

        $introspected = client_catalog_provider($this->client, ['public'])
            ->get()
            ->get('public')
            ->table($this->tableName);

        static::assertTrue($introspected->unlogged);
    }
}
