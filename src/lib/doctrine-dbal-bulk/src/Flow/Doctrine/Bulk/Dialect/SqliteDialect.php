<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Flow\Doctrine\Bulk\{BulkData, Columns, TableDefinition};

final class SqliteDialect implements Dialect
{
    public function __construct(private readonly AbstractPlatform $platform)
    {
    }

    /**
     * @psalm-suppress MoreSpecificImplementedParamType
     *
     * @param array{
     *  skip_conflicts?: boolean,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>
     * } $insertOptions
     */
    public function prepareInsert(TableDefinition $table, BulkData $bulkData, array $insertOptions = []) : string
    {
        if (\array_key_exists('conflict_columns', $insertOptions)) {
            return \sprintf(
                'INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s',
                $table->name(),
                \implode(',', \array_map(fn (string $column) : string => $this->platform->quoteIdentifier($column), $bulkData->columns()->all())),
                $bulkData->toSqlPlaceholders(),
                \implode(',', $insertOptions['conflict_columns']),
                (\array_key_exists('update_columns', $insertOptions) && [] !== $insertOptions['update_columns'])
                    ? $this->updateSelectedColumns($insertOptions['update_columns'], $bulkData->columns())
                    : $this->updateAllColumns($bulkData->columns())
            );
        }

        if (\array_key_exists('skip_conflicts', $insertOptions) && $insertOptions['skip_conflicts'] === true) {
            return \sprintf(
                'INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING',
                $table->name(),
                \implode(',', \array_map(fn (string $column) : string => $this->platform->quoteIdentifier($column), $bulkData->columns()->all())),
                $bulkData->toSqlPlaceholders()
            );
        }

        return \sprintf(
            'INSERT INTO %s (%s) VALUES %s',
            $table->name(),
            \implode(',', \array_map(fn (string $column) : string => $this->platform->quoteIdentifier($column), $bulkData->columns()->all())),
            $bulkData->toSqlPlaceholders()
        );
    }

    public function prepareUpdate(TableDefinition $table, BulkData $bulkData, array $updateOptions = []) : string
    {
        return \sprintf(
            'REPLACE INTO %s (%s) VALUES %s',
            $table->name(),
            \implode(',', \array_map(fn (string $column) : string => $this->platform->quoteIdentifier($column), $bulkData->columns()->all())),
            $bulkData->toSqlPlaceholders()
        );
    }

    private function updateAllColumns(Columns $columns) : string
    {
        return \implode(
            ',',
            $columns->map(
                fn (string $column) : string => "{$this->platform->quoteIdentifier($column)} = {$this->platform->quoteIdentifier('excluded.' . $column)}"
            )
        );
    }

    /**
     * @param array<string> $updateColumns
     */
    private function updateSelectedColumns(array $updateColumns, Columns $columns) : string
    {
        return [] !== $updateColumns
            ? \implode(',', \array_map(fn (string $column) : string => "{$this->platform->quoteIdentifier($column)} = {$this->platform->quoteIdentifier('excluded.' . $column)}", $updateColumns))
            : $this->updateAllColumns($columns);
    }
}
