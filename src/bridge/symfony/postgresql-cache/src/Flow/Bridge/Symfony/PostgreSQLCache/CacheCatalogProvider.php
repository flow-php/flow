<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache;

use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\CatalogProvider;

use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_column_bytea;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_varchar;
use function Flow\PostgreSql\DSL\schema_index;
use function Flow\PostgreSql\DSL\schema_primary_key;
use function Flow\PostgreSql\DSL\schema_table;

final readonly class CacheCatalogProvider implements CatalogProvider
{
    public function __construct(
        private string $tableName = 'cache_items',
        private string $schemaName = 'public',
        private string $idCol = 'item_id',
        private string $dataCol = 'item_data',
        private string $lifetimeCol = 'item_lifetime',
        private string $timeCol = 'item_time',
    ) {}

    public function get(): Catalog
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
                        schema_index('idx_' . $this->tableName . '_lifetime_time', [
                            $this->lifetimeCol,
                            $this->timeCol,
                        ]),
                    ],
                    schema: $this->schemaName,
                ),
            ]),
        ]);
    }
}
