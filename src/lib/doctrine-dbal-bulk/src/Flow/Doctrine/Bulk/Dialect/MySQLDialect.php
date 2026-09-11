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
use function current;
use function implode;
use function sprintf;

final readonly class MySQLDialect implements Dialect
{
    public function __construct(
        private AbstractPlatform $platform,
    ) {}

    public function maxBindParameters(): int
    {
        // COM_STMT_EXECUTE carries the parameter count in 2 bytes
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
            $bulkData->toSqlPlaceholders(),
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
            $options = new MySQLInsertOptions();
        }

        if (!$options instanceof MySQLInsertOptions) {
            throw new RuntimeException('Invalid insert options provided, expected MySQLInsertOptions got: '
            . $options::class);
        }

        if ($options->skipConflicts === true) {
            return sprintf(
                'INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %4$s=%4$s',
                $table->name(),
                // @mago-expect analysis:deprecated-method
                implode(',', array_map(fn(string $column): string => $this->platform->quoteIdentifier(
                    $column,
                ), $bulkData->columns()->all())),
                $bulkData->toSqlPlaceholders(),
                current($bulkData->columns()->all()),
            );
        }

        if ($options->upsert === true) {
            return sprintf(
                'INSERT INTO %s (%s)
                VALUES %s
                ON DUPLICATE KEY UPDATE %s',
                $table->name(),
                // @mago-expect analysis:deprecated-method
                implode(',', array_map(fn(string $column): string => $this->platform->quoteIdentifier(
                    $column,
                ), $bulkData->columns()->all())),
                $bulkData->toSqlPlaceholders(),
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
     * @param null|UpdateOptions $options
     *
     * @return string
     */
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

    /**
     * @param Columns $columns
     *
     * @return string
     */
    private function updateAllColumns(Columns $columns): string
    {
        return implode(
            ',',
            $columns->map(
                // @mago-expect analysis:deprecated-method
                // @mago-expect analysis:deprecated-method
                fn(string $column): string => "{$this->platform->quoteIdentifier(
                    $column,
                )} = VALUES({$this->platform->quoteIdentifier($column)})",
            ),
        );
    }

    /**
     * @param array<string> $updateColumns
     * @param Columns $columns
     *
     * @return string
     */
    private function updateSelectedColumns(
        array $updateColumns,
        Columns $columns,
        string $tableName,
        ?bool $preserveExistingValues = null,
    ): string {
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
                                . "COALESCE(VALUES({$this->platform->quoteIdentifier(
                                    $column,
                                )}), {$tableName}.{$this->platform->quoteIdentifier($column)})"
                            );
                        }

                        // @mago-expect analysis:deprecated-method
                        return $clause . "VALUES({$this->platform->quoteIdentifier($column)})";
                    },
                    $updateColumns,
                ))
                : $this->updateAllColumns($columns)
        );
    }
}
