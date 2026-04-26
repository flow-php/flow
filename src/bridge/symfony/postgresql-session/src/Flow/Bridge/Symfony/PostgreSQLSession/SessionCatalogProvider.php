<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession;

use function Flow\PostgreSql\DSL\{schema, schema_column_bytea, schema_column_integer, schema_column_varchar, schema_index, schema_primary_key, schema_table};

use Flow\PostgreSql\Schema\{Catalog, CatalogProvider};

final readonly class SessionCatalogProvider implements CatalogProvider
{
    public function __construct(
        private string $tableName = 'sessions',
        private string $schemaName = 'public',
        private string $idCol = 'sess_id',
        private string $dataCol = 'sess_data',
        private string $lifetimeCol = 'sess_lifetime',
        private string $timeCol = 'sess_time',
    ) {
    }

    public function get() : Catalog
    {
        return new Catalog([
            schema($this->schemaName, [
                schema_table(
                    name: $this->tableName,
                    columns: [
                        schema_column_varchar($this->idCol, 128, nullable: false),
                        schema_column_bytea($this->dataCol, nullable: false),
                        schema_column_integer($this->lifetimeCol, nullable: false),
                        schema_column_integer($this->timeCol, nullable: false),
                    ],
                    primaryKey: schema_primary_key([$this->idCol]),
                    indexes: [
                        schema_index('idx_' . $this->tableName . '_lifetime', [$this->lifetimeCol]),
                    ],
                    schema: $this->schemaName,
                ),
            ]),
        ]);
    }
}
