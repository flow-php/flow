<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Columns;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Doctrine\Bulk\TableDefinition;

final class MySQLDialect implements Dialect
{
    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param array{
     *  skip_conflicts?: boolean,
     *  upsert?: boolean,
     * } $insertOptions $insertOptions
     *
     * @return string
     */
    public function prepareInsert(TableDefinition $table, BulkData $bulkData, array $insertOptions = []) : string
    {
        if (\array_key_exists('skip_conflicts', $insertOptions) && $insertOptions['skip_conflicts'] === true) {
            return \sprintf(
                'INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %4$s=%4$s',
                $table->name(),
                $bulkData->columns()->concat(','),
                $bulkData->toSqlPlaceholders(),
                \current($bulkData->columns()->all())
            );
        }

        if (\array_key_exists('upsert', $insertOptions) && $insertOptions['upsert'] === true) {
            return \sprintf(
                'INSERT INTO %s (%s) 
                VALUES %s 
                ON DUPLICATE KEY UPDATE %s',
                $table->name(),
                $bulkData->columns()->concat(','),
                $bulkData->toSqlPlaceholders(),
                \array_key_exists('update_columns', $insertOptions) && \count($insertOptions['update_columns'])
                    ? $this->updateSelectedColumns($insertOptions['update_columns'], $bulkData->columns())
                    : $this->updateAllColumns($bulkData->columns())
            );
        }

        return \sprintf(
            'INSERT INTO %s (%s) VALUES %s',
            $table->name(),
            $bulkData->columns()->concat(','),
            $bulkData->toSqlPlaceholders()
        );
    }

    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param array $updateOptions
     *
     * @throws RuntimeException
     *
     * @return string
     */
    public function prepareUpdate(TableDefinition $table, BulkData $bulkData, array $updateOptions = []) : string
    {
        return \sprintf(
            'REPLACE INTO %s (%s) VALUES %s',
            $table->name(),
            $bulkData->columns()->concat(','),
            $bulkData->toSqlPlaceholders()
        );
    }

    /**
     * @param array<string> $updateColumns
     * @param Columns $columns
     *
     * @return string
     */
    private function replaceSelectedColumns(array $updateColumns, Columns $columns) : string
    {
        return \count($updateColumns)
            ? \implode(',', \array_map(fn (string $column) : string => "{$column} = VALUES({$column})", $updateColumns))
            : $this->updateAllColumns($columns);
    }

    /**
     * @param Columns $columns
     *
     * @return string
     */
    private function updateAllColumns(Columns $columns) : string
    {
        return \implode(
            ',',
            $columns->map(
                fn (string $column) : string => "{$column} = VALUES({$column})"
            )
        );
    }

    /**
     * @param array<string> $updateColumns
     * @param Columns $columns
     *
     * @return string
     */
    private function updateSelectedColumns(array $updateColumns, Columns $columns) : string
    {
        return \count($updateColumns)
            ? \implode(',', \array_map(fn (string $column) : string => "{$column} = VALUES({$column})", $updateColumns))
            : $this->updateAllColumns($columns);
    }
}
