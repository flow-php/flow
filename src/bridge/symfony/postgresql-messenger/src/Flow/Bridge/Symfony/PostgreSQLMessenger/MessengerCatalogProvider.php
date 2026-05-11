<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger;

use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\CatalogProvider;
use Flow\PostgreSql\Schema\IdentityGeneration;

use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_column;
use function Flow\PostgreSql\DSL\schema_column_text;
use function Flow\PostgreSql\DSL\schema_column_timestamp_tz;
use function Flow\PostgreSql\DSL\schema_column_varchar;
use function Flow\PostgreSql\DSL\schema_index;
use function Flow\PostgreSql\DSL\schema_primary_key;
use function Flow\PostgreSql\DSL\schema_table;

final readonly class MessengerCatalogProvider implements CatalogProvider
{
    public function __construct(
        private string $tableName = 'messenger_messages',
        private string $schemaName = 'public',
    ) {}

    public function get(): Catalog
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
                        schema_column_varchar('queue_name', 190, nullable: false, default: 'default'),
                        schema_column_timestamp_tz('created_at', nullable: false),
                        schema_column_timestamp_tz('available_at', nullable: false),
                        schema_column_timestamp_tz('delivered_at', nullable: true),
                    ],
                    primaryKey: schema_primary_key(['id']),
                    indexes: [
                        schema_index('idx_' . $this->tableName . '_queue_name', ['queue_name']),
                        schema_index('idx_' . $this->tableName . '_available_at', ['available_at']),
                        schema_index('idx_' . $this->tableName . '_delivered_at', ['delivered_at']),
                    ],
                    schema: $this->schemaName,
                ),
            ]),
        ]);
    }
}
