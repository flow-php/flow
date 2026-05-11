<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession;

use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\CatalogProvider;

use function Flow\PostgreSql\DSL\schema;
use function Flow\PostgreSql\DSL\schema_column_bytea;
use function Flow\PostgreSql\DSL\schema_column_integer;
use function Flow\PostgreSql\DSL\schema_column_varchar;
use function Flow\PostgreSql\DSL\schema_index;
use function Flow\PostgreSql\DSL\schema_primary_key;
use function Flow\PostgreSql\DSL\schema_table;

final readonly class SessionCatalogProvider implements CatalogProvider
{
    public function __construct(
        private string $tableName = 'sessions',
        private string $schemaName = 'public',
        private string $idCol = 'sess_id',
        private string $dataCol = 'sess_data',
        private string $lifetimeCol = 'sess_lifetime',
        private string $timeCol = 'sess_time',
    ) {}

    public function get(): Catalog
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
