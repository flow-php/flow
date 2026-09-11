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

final readonly class SqliteDialect implements Dialect
{
    public function __construct(
        private AbstractPlatform $platform,
    ) {}

    public function maxBindParameters(): int
    {
        // SQLITE_MAX_VARIABLE_NUMBER since SQLite 3.32.0 (999 before)
        return 32_766;
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
            $bulkData->toSqlPlaceholders(),
        );
    }

    public function prepareInsert(TableDefinition $table, BulkData $bulkData, ?InsertOptions $options = null): string
    {
        if ($options === null) {
            $options = new SqliteInsertOptions();
        }

        if (!$options instanceof SqliteInsertOptions) {
            throw new RuntimeException('Invalid insert options provided, expected MySQLInsertOptions got: '
            . $options::class);
        }

        if ($options->conflictColumns) {
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
                    ? $this->updateSelectedColumns(
                        $options->updateColumns,
                        $bulkData->columns(),
                        $table->name(),
                        $options->preserveExistingValues,
                    )
                    : $this->updateAllColumns($bulkData->columns()),
            );
        }

        if ($options->skipConflicts) {
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

    public function prepareUpdate(TableDefinition $table, BulkData $bulkData, ?UpdateOptions $options = null): string
    {
        return sprintf(
            'REPLACE INTO %s (%s) VALUES %s',
            $table->name(),
            // @mago-expect analysis:deprecated-method
            implode(',', array_map(fn(string $column): string => $this->platform->quoteIdentifier(
                $column,
            ), $bulkData->columns()->all())),
            $bulkData->toSqlPlaceholders(),
        );
    }

    private function updateAllColumns(Columns $columns): string
    {
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
    private function updateSelectedColumns(
        array $updateColumns,
        Columns $columns,
        string $tableName,
        ?bool $preserveExistingValues = null,
    ): string {
        return (
            [] !== $updateColumns
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
