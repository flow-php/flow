<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\TableDefinition;

final class PostgreSQLDialect implements Dialect
{
    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param array{
     *  skip_conflicts?: boolean,
     *  constraint?: string,
     *  conflict_columns?: array<string>,
     *  update_columns?: array<string>
     * } $insertOptions $insertOptions
     *
     * @return string
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
                (\array_key_exists('update_columns', $insertOptions) && \count($insertOptions['update_columns']))
                    ? $this->updatedSelectedColumns($insertOptions['update_columns'], $bulkData)
                    : $this->updateAllColumns($bulkData)
            );
        }

        if (\array_key_exists('constraint', $insertOptions)) {
            return \sprintf(
                'INSERT INTO %s (%s) VALUES %s ON CONFLICT ON CONSTRAINT %s DO UPDATE SET %s',
                $table->name(),
                $bulkData->columns()->concat(','),
                $bulkData->toSqlPlaceholders(),
                $insertOptions['constraint'],
                (\array_key_exists('update_columns', $insertOptions) && \count($insertOptions['update_columns']))
                    ? $this->updatedSelectedColumns($insertOptions['update_columns'], $bulkData)
                    : $this->updateAllColumns($bulkData)
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

    /**
     * @param array<string> $updateColumns
     * @param BulkData $bulkData
     *
     * @return string
     */
    private function updatedSelectedColumns(array $updateColumns, BulkData $bulkData) : string
    {
        /**
         * https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT
         * The SET and WHERE clauses in ON CONFLICT DO UPDATE have access to the existing row using the
         * table's name (or an alias), and to rows proposed for insertion using the special EXCLUDED table.
         */
        return \count($updateColumns)
            ? \implode(',', \array_map(fn (string $column) : string => "{$column} = excluded.{$column}", $updateColumns))
            : $this->updateAllColumns($bulkData);
    }

    /**
     * @param BulkData $bulkData
     *
     * @return string
     */
    private function updateAllColumns(BulkData $bulkData) : string
    {
        /**
         * https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT
         * The SET and WHERE clauses in ON CONFLICT DO UPDATE have access to the existing row using the
         * table's name (or an alias), and to rows proposed for insertion using the special EXCLUDED table.
         */
        return \implode(
            ',',
            $bulkData->columns()->map(
                fn (string $column) : string => "{$column} = excluded.{$column}"
            )
        );
    }
}
