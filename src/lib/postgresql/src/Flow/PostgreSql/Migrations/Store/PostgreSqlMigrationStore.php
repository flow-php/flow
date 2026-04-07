<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Store;

use function Flow\PostgreSql\DSL\{and_,
    asc,
    col,
    column,
    column_type_integer,
    column_type_timestamptz,
    column_type_varchar,
    create,
    delete,
    eq,
    func,
    insert,
    param,
    parameters,
    select};

use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Migrations\{Configuration, ExecutedMigration, Version};

final readonly class PostgreSqlMigrationStore implements MigrationStore
{
    public function __construct(
        private Client $client,
        private Configuration $configuration,
    ) {
    }

    public function complete(Version $version, int $executionTimeMs) : void
    {
        $this->client->execute(
            insert()->into($this->qualifiedTableName())
                ->columns('version', 'execution_time_ms')
                ->values(...parameters(2)),
            [(string) $version, $executionTimeMs],
        );
    }

    public function executedMigrations() : ExecutedMigrations
    {
        $rows = $this->client->fetchAll(
            select(col('version'), col('executed_at'), col('execution_time_ms'))
                ->from($this->qualifiedTableName())
                ->orderBy(asc('version')),
        );

        return new ExecutedMigrations(
            ...\array_map(
                static function (array $row) : ExecutedMigration {
                    /** @var string $version */
                    $version = $row['version'];
                    /** @var string $executedAt */
                    $executedAt = $row['executed_at'];
                    /** @var null|int $executionTimeMs */
                    $executionTimeMs = $row['execution_time_ms'];

                    return new ExecutedMigration(
                        Version::fromString($version),
                        new \DateTimeImmutable($executedAt),
                        $executionTimeMs,
                    );
                },
                $rows,
            ),
        );
    }

    public function initialize() : void
    {
        $this->client->execute(
            create()->table($this->configuration->tableName, $this->configuration->tableSchema)
                ->column(column('version', column_type_varchar(255))->primaryKey())
                ->column(column('executed_at', column_type_timestamptz())->notNull()->default(func('now')))
                ->column(column('execution_time_ms', column_type_integer()))
                ->ifNotExists(),
        );
    }

    public function isInitialized() : bool
    {
        return $this->client->fetch(
            select(col('table_name'))
                ->from('information_schema.tables')
                ->where(and_(
                    eq(col('table_schema'), param(1)),
                    eq(col('table_name'), param(2)),
                )),
            [$this->configuration->tableSchema, $this->configuration->tableName],
        ) !== null;
    }

    public function remove(Version $version) : void
    {
        $this->client->execute(
            delete()->from($this->qualifiedTableName())
                ->where(eq(col('version'), param(1))),
            [(string) $version],
        );
    }

    public function reset() : void
    {
        $this->client->execute(
            delete()->from($this->qualifiedTableName()),
        );
    }

    private function qualifiedTableName() : string
    {
        return $this->configuration->tableSchema . '.' . $this->configuration->tableName;
    }
}
