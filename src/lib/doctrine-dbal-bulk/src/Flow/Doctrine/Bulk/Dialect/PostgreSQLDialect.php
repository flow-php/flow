<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Columns;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Doctrine\Bulk\TableDefinition;

final class PostgreSQLDialect implements Dialect
{
    private AbstractPlatform $platform;

    public function __construct(AbstractPlatform $platform)
    {
        $this->platform = $platform;
    }

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
                    ? $this->updatedSelectedColumns($insertOptions['update_columns'], $bulkData->columns())
                    : $this->updateAllColumns($bulkData->columns())
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
                    ? $this->updatedSelectedColumns($insertOptions['update_columns'], $bulkData->columns())
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

    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     * @param array{
     *  primary_key_columns?: array<string>,
     *  update_columns?: array<string>
     * } $updateOptions $updateOptions
     *
     * @throws RuntimeException
     *
     * @return string
     */
    public function prepareUpdate(TableDefinition $table, BulkData $bulkData, array $updateOptions = []) : string
    {
        if (false === \array_key_exists('primary_key_columns', $updateOptions)) {
            throw new RuntimeException('primary_key_columns option is required for update.');
        }

        if (false === $bulkData->columns()->has(...$updateOptions['primary_key_columns'])) {
            throw new RuntimeException('All columns from primary_key_columns must be in bulk data columns.');
        }

        return \sprintf(
            'UPDATE %s as existing_table SET %s FROM (VALUES %s) as excluded (%s) WHERE %s',
            $table->name(),
            (\array_key_exists('update_columns', $updateOptions) && \count($updateOptions['update_columns']))
                ? $this->updatedSelectedColumns($updateOptions['update_columns'], $bulkData->columns()->without(...$updateOptions['primary_key_columns']))
                : $this->updateAllColumns($bulkData->columns()->without(...$updateOptions['primary_key_columns'])),
            $table->toSqlCastedPlaceholders($bulkData, $this->platform),
            $bulkData->columns()->concat(','),
            $this->updatedIndexColumns($updateOptions['primary_key_columns'])
        );
    }

    /**
     * @param Columns $columns
     *
     * @return string
     */
    private function updateAllColumns(Columns $columns) : string
    {
        /**
         * https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT
         * The SET and WHERE clauses in ON CONFLICT DO UPDATE have access to the existing row using the
         * table's name (or an alias), and to rows proposed for insertion using the special EXCLUDED table.
         */
        return \implode(
            ',',
            $columns->map(
                fn (string $column) : string => "{$column} = excluded.{$column}"
            )
        );
    }

    /**
     * @param array<string> $updateColumns
     *
     * @return string
     */
    private function updatedIndexColumns(array $updateColumns) : string
    {
        return \implode(' AND ', \array_map(fn (string $column) : string => "existing_table.{$column} = excluded.{$column}", $updateColumns));
    }

    /**
     * @param array<string> $updateColumns
     * @param Columns $columns
     *
     * @return string
     */
    private function updatedSelectedColumns(array $updateColumns, Columns $columns) : string
    {
        /**
         * https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT
         * The SET and WHERE clauses in ON CONFLICT DO UPDATE have access to the existing row using the
         * table's name (or an alias), and to rows proposed for insertion using the special EXCLUDED table.
         */
        return \count($updateColumns)
            ? \implode(',', \array_map(fn (string $column) : string => "{$column} = excluded.{$column}", $updateColumns))
            : $this->updateAllColumns($columns);
    }
}
