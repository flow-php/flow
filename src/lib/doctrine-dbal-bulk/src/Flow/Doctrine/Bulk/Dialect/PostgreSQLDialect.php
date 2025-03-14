<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Doctrine\Bulk\{BulkData, Columns, InsertOptions, TableDefinition, UpdateOptions};

final readonly class PostgreSQLDialect implements Dialect
{
    public function __construct(private AbstractPlatform $platform)
    {
    }

    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     *
     * @return string
     */
    public function prepareInsert(TableDefinition $table, BulkData $bulkData, ?InsertOptions $options = null) : string
    {
        if ($options === null) {
            $options = new PostgreSQLInsertOptions();
        }

        if (!$options instanceof PostgreSQLInsertOptions) {
            throw new RuntimeException('Invalid insert options provided, expected PostgreSQLInsertOptions got: ' . $options::class);
        }

        if (\count($options->conflictColumns)) {
            return \sprintf(
                'INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s',
                $table->name(),
                \implode(',', \array_map(fn (string $column) : string => $this->platform->quoteIdentifier($column), $bulkData->columns()->all())),
                $bulkData->toSqlPlaceholders(),
                \implode(',', $options->conflictColumns),
                \count($options->updateColumns)
                    ? $this->updatedSelectedColumns($options->updateColumns, $bulkData->columns())
                    : $this->updateAllColumns($bulkData->columns())
            );
        }

        if ($options->constraint) {
            return \sprintf(
                'INSERT INTO %s (%s) VALUES %s ON CONFLICT ON CONSTRAINT %s DO UPDATE SET %s',
                $table->name(),
                \implode(',', \array_map(fn (string $column) : string => $this->platform->quoteIdentifier($column), $bulkData->columns()->all())),
                $bulkData->toSqlPlaceholders(),
                $options->constraint,
                \count($options->updateColumns)
                    ? $this->updatedSelectedColumns($options->updateColumns, $bulkData->columns())
                    : $this->updateAllColumns($bulkData->columns())
            );
        }

        if ($options->skipConflicts === true) {
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

    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     *
     * @throws RuntimeException
     *
     * @return string
     */
    public function prepareUpdate(TableDefinition $table, BulkData $bulkData, ?UpdateOptions $options = null) : string
    {
        if ($options === null) {
            $options = new PostgreSQLUpdateOptions();
        }

        if (!$options instanceof PostgreSQLUpdateOptions) {
            throw new RuntimeException('Invalid update options provided, expected UpdateOptions got: ' . $options::class);
        }

        if (!\count($options->primaryKeyColumns)) {
            throw new RuntimeException('primary_key_columns option is required for update.');
        }

        if (false === $bulkData->columns()->has(...$options->primaryKeyColumns)) {
            throw new RuntimeException('All columns from primary_key_columns must be in bulk data columns.');
        }

        return \sprintf(
            'UPDATE %s as existing_table SET %s FROM (VALUES %s) as excluded (%s) WHERE %s',
            $table->name(),
            \count($options->updateColumns)
                ? $this->updatedSelectedColumns($options->updateColumns, $bulkData->columns()->without(...$options->primaryKeyColumns))
                : $this->updateAllColumns($bulkData->columns()->without(...$options->primaryKeyColumns)),
            $table->toSqlCastedPlaceholders($bulkData, $this->platform),
            \implode(',', \array_map(fn (string $column) : string => $this->platform->quoteIdentifier($column), $bulkData->columns()->all())),
            $this->updatedIndexColumns($options->primaryKeyColumns)
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
                fn (string $column) : string => "{$this->platform->quoteIdentifier($column)} = {$this->platform->quoteIdentifier('excluded.' . $column)}"
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
        return \implode(' AND ', \array_map(fn (string $column) : string => "{$this->platform->quoteIdentifier('existing_table.' . $column)} = {$this->platform->quoteIdentifier('excluded.' . $column)}", $updateColumns));
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
            ? \implode(',', \array_map(fn (string $column) : string => "{$this->platform->quoteIdentifier($column)} = {$this->platform->quoteIdentifier('excluded.' . $column)}", $updateColumns))
            : $this->updateAllColumns($columns);
    }
}
