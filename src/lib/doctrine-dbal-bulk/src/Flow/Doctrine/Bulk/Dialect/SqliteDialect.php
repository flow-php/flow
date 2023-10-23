<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Columns;
use Flow\Doctrine\Bulk\TableDefinition;

final class SqliteDialect implements Dialect
{
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
                $bulkData->columns()->concat(','),
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
                $bulkData->columns()->concat(','),
                $bulkData->toSqlPlaceholders()
            );
        }

        return \sprintf(
            'INSERT INTO %s (%s) VALUES %s',
            $table->name(),
            $bulkData->columns()->concat(','),
            $bulkData->toSqlPlaceholders()
        );
    }

    public function prepareUpdate(TableDefinition $table, BulkData $bulkData, array $updateOptions = []) : string
    {
        return \sprintf(
            'REPLACE INTO %s (%s) VALUES %s',
            $table->name(),
            $bulkData->columns()->concat(','),
            $bulkData->toSqlPlaceholders()
        );
    }

    private function updateAllColumns(Columns $columns) : string
    {
        return \implode(
            ',',
            $columns->map(
                fn (string $column) : string => "{$column} = excluded.{$column}"
            )
        );
    }

    /**
     * @param array<string> $updateColumns
     */
    private function updateSelectedColumns(array $updateColumns, Columns $columns) : string
    {
        return [] !== $updateColumns
            ? \implode(',', \array_map(static fn (string $column) : string => "{$column} = excluded.{$column}", $updateColumns))
            : $this->updateAllColumns($columns);
    }
}
