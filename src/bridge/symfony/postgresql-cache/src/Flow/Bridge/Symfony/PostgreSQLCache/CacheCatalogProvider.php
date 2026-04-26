<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache;

use function Flow\PostgreSql\DSL\{schema, schema_column_bytea, schema_column_integer, schema_column_varchar, schema_index, schema_primary_key, schema_table};

use Flow\PostgreSql\Schema\{Catalog, CatalogProvider};

final readonly class CacheCatalogProvider implements CatalogProvider
{
    public function __construct(
        private string $tableName = 'cache_items',
        private string $schemaName = 'public',
        private string $idCol = 'item_id',
        private string $dataCol = 'item_data',
        private string $lifetimeCol = 'item_lifetime',
        private string $timeCol = 'item_time',
    ) {
    }

    public function get() : Catalog
    {
        return new Catalog([
            schema($this->schemaName, [
                schema_table(
                    name: $this->tableName,
                    columns: [
                        schema_column_varchar($this->idCol, 255, nullable: false),
                        schema_column_bytea($this->dataCol, nullable: false),
                        schema_column_integer($this->lifetimeCol, nullable: true),
                        schema_column_integer($this->timeCol, nullable: false),
                    ],
                    primaryKey: schema_primary_key([$this->idCol]),
                    indexes: [
                        schema_index('idx_' . $this->tableName . '_lifetime_time', [$this->lifetimeCol, $this->timeCol]),
                    ],
                    schema: $this->schemaName,
                ),
            ]),
        ]);
    }
}
