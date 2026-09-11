<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Flow\Doctrine\Bulk\BulkData;
use Flow\Doctrine\Bulk\Columns;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Doctrine\Bulk\InsertOptions;
use Flow\Doctrine\Bulk\TableDefinition;
use Flow\Doctrine\Bulk\UpdateOptions;

use function array_map;
use function count;
use function implode;
use function sprintf;

final readonly class PostgreSQLDialect implements Dialect
{
    public function __construct(
        private AbstractPlatform $platform,
    ) {}

    public function maxBindParameters(): int
    {
        // PQ_QUERY_PARAM_MAX_LIMIT in libpq-fe.h, inclusive, checked per PQsendQueryParams
        return 65_535;
    }

    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     *
     * @return string
     */
    public function prepareDelete(TableDefinition $table, BulkData $bulkData): string
    {
        $columns = $bulkData->columns()->all();

        return sprintf(
            'DELETE FROM %s WHERE (%s) IN (%s)',
            $table->name(),
            // @mago-expect analysis:deprecated-method
            implode(', ', array_map(fn($column) => $this->platform->quoteIdentifier($column), $columns)),
            $bulkData->toSqlCastedPlaceholders($table),
        );
    }

    /**
     * @param TableDefinition $table
     * @param BulkData $bulkData
     *
     * @return string
     */
    public function prepareInsert(TableDefinition $table, BulkData $bulkData, ?InsertOptions $options = null): string
    {
        if ($options === null) {
            $options = new PostgreSQLInsertOptions();
        }

        if (!$options instanceof PostgreSQLInsertOptions) {
            throw new RuntimeException('Invalid insert options provided, expected PostgreSQLInsertOptions got: '
            . $options::class);
        }

        if (count($options->conflictColumns)) {
            return sprintf(
                'INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s',
                $table->name(),
                // @mago-expect analysis:deprecated-method
                implode(',', array_map(fn(string $column): string => $this->platform->quoteIdentifier(
                    $column,
                ), $bulkData->columns()->all())),
                $bulkData->toSqlPlaceholders(),
                implode(',', $options->conflictColumns),
                count($options->updateColumns)
                    ? $this->updatedSelectedColumns(
                        $options->updateColumns,
                        $bulkData->columns(),
                        $table->name(),
                        $options->preserveExistingValues,
                    )
                    : $this->updateAllColumns($bulkData->columns()),
            );
        }

        if ($options->constraint) {
            return sprintf(
                'INSERT INTO %s (%s) VALUES %s ON CONFLICT ON CONSTRAINT %s DO UPDATE SET %s',
                $table->name(),
                // @mago-expect analysis:deprecated-method
                implode(',', array_map(fn(string $column): string => $this->platform->quoteIdentifier(
                    $column,
                ), $bulkData->columns()->all())),
                $bulkData->toSqlPlaceholders(),
                $options->constraint,
                count($options->updateColumns)
                    ? $this->updatedSelectedColumns(
                        $options->updateColumns,
                        $bulkData->columns(),
                        $table->name(),
                        $options->preserveExistingValues,
                    )
                    : $this->updateAllColumns($bulkData->columns()),
            );
        }

        if ($options->skipConflicts === true) {
            return sprintf(
                'INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING',
                $table->name(),
                // @mago-expect analysis:deprecated-method
                implode(',', array_map(fn(string $column): string => $this->platform->quoteIdentifier(
                    $column,
                ), $bulkData->columns()->all())),
                $bulkData->toSqlPlaceholders(),
            );
        }

        return sprintf(
            'INSERT INTO %s (%s) VALUES %s',
            $table->name(),
            // @mago-expect analysis:deprecated-method
            implode(',', array_map(fn(string $column): string => $this->platform->quoteIdentifier(
                $column,
            ), $bulkData->columns()->all())),
            $bulkData->toSqlPlaceholders(),
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
    public function prepareUpdate(TableDefinition $table, BulkData $bulkData, ?UpdateOptions $options = null): string
    {
        if ($options === null) {
            $options = new PostgreSQLUpdateOptions();
        }

        if (!$options instanceof PostgreSQLUpdateOptions) {
            throw new RuntimeException('Invalid update options provided, expected UpdateOptions got: '
            . $options::class);
        }

        if (!count($options->primaryKeyColumns)) {
            throw new RuntimeException('primary_key_columns option is required for update.');
        }

        if (false === $bulkData->columns()->has(...$options->primaryKeyColumns)) {
            throw new RuntimeException('All columns from primary_key_columns must be in bulk data columns.');
        }

        return sprintf(
            'UPDATE %s as existing_table SET %s FROM (VALUES %s) as excluded (%s) WHERE %s',
            $table->name(),
            count($options->updateColumns)
                ? $this->updatedSelectedColumns(
                    $options->updateColumns,
                    $bulkData->columns()->without(...$options->primaryKeyColumns),
                    $table->name(),
                    $options->preserveExistingValues,
                )
                : $this->updateAllColumns($bulkData->columns()->without(...$options->primaryKeyColumns)),
            $bulkData->toSqlCastedPlaceholders($table),
            // @mago-expect analysis:deprecated-method
            implode(',', array_map(fn(string $column): string => $this->platform->quoteIdentifier(
                $column,
            ), $bulkData->columns()->all())),
            $this->updatedIndexColumns($options->primaryKeyColumns),
        );
    }

    private function updateAllColumns(Columns $columns): string
    {
        /**
         * https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT
         * The SET and WHERE clauses in ON CONFLICT DO UPDATE have access to the existing row using the
         * table's name (or an alias), and to rows proposed for insertion using the special EXCLUDED table.
         */
        return implode(
            ',',
            $columns->map(
                // @mago-expect analysis:deprecated-method
                // @mago-expect analysis:deprecated-method
                fn(string $column): string => "{$this->platform->quoteIdentifier(
                    $column,
                )} = {$this->platform->quoteIdentifier('excluded.' . $column)}",
            ),
        );
    }

    /**
     * @param array<string> $updateColumns
     */
    private function updatedIndexColumns(array $updateColumns): string
    {
        return implode(' AND ', array_map(
            // @mago-expect analysis:deprecated-method
            // @mago-expect analysis:deprecated-method
            fn(string $column): string => "{$this->platform->quoteIdentifier('existing_table.'
            . $column)} = {$this->platform->quoteIdentifier('excluded.' . $column)}",
            $updateColumns,
        ));
    }

    /**
     * @param array<string> $updateColumns
     */
    private function updatedSelectedColumns(
        array $updateColumns,
        Columns $columns,
        string $tableName,
        ?bool $preserveExistingValues = null,
    ): string {
        /**
         * https://www.postgresql.org/docs/9.5/sql-insert.html#SQL-ON-CONFLICT
         * The SET and WHERE clauses in ON CONFLICT DO UPDATE have access to the existing row using the
         * table's name (or an alias), and to rows proposed for insertion using the special EXCLUDED table.
         */
        return (
            count($updateColumns)
                ? implode(',', array_map(
                    function (string $column) use ($tableName, $preserveExistingValues): string {
                        // @mago-expect analysis:deprecated-method
                        $clause = "{$this->platform->quoteIdentifier($column)} = ";

                        if (true === $preserveExistingValues) {
                            return (
                                $clause
                                // @mago-expect analysis:deprecated-method
                                // @mago-expect analysis:deprecated-method
                                . "COALESCE({$this->platform->quoteIdentifier('excluded.'
                                . $column)}, {$tableName}.{$this->platform->quoteIdentifier($column)})"
                            );
                        }

                        // @mago-expect analysis:deprecated-method
                        return $clause . "{$this->platform->quoteIdentifier('excluded.' . $column)}";
                    },
                    $updateColumns,
                ))
                : $this->updateAllColumns($columns)
        );
    }
}
