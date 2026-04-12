<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger;

use function Flow\PostgreSql\DSL\{schema, schema_column, schema_column_text, schema_column_timestamp_tz, schema_column_varchar, schema_index, schema_primary_key, schema_table};

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\{Catalog, CatalogProvider, IdentityGeneration};

final readonly class MessengerCatalogProvider implements CatalogProvider
{
    public function __construct(
        private string $tableName = 'messenger_messages',
        private string $schemaName = 'public',
    ) {
    }

    public function get() : Catalog
    {
        return new Catalog([
            schema($this->schemaName, [
                schema_table(
                    name: $this->tableName,
                    columns: [
                        schema_column(
                            name: 'id',
                            type: ColumnType::bigint(),
                            nullable: false,
                            isIdentity: true,
                            identityGeneration: IdentityGeneration::ALWAYS,
                        ),
                        schema_column_text('body', nullable: false),
                        schema_column_text('headers', nullable: false),
                        schema_column_varchar('queue_name', 190, nullable: false),
                        schema_column_timestamp_tz('created_at', nullable: false),
                        schema_column_timestamp_tz('available_at', nullable: false),
                        schema_column_timestamp_tz('delivered_at', nullable: true),
                    ],
                    primaryKey: schema_primary_key(['id']),
                    indexes: [
                        schema_index($this->tableName . '_queue_name_idx', ['queue_name']),
                        schema_index($this->tableName . '_available_at_idx', ['available_at']),
                        schema_index($this->tableName . '_delivered_at_idx', ['delivered_at']),
                    ],
                    schema: $this->schemaName,
                ),
            ]),
        ]);
    }
}
