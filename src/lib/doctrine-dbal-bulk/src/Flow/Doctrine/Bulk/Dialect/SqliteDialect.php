<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Dialect;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Flow\Doctrine\Bulk\{BulkData, Columns, Exception\RuntimeException, InsertOptions, TableDefinition, UpdateOptions};

final readonly class SqliteDialect implements Dialect
{
    public function __construct(private AbstractPlatform $platform)
    {
    }

    public function prepareInsert(TableDefinition $table, BulkData $bulkData, ?InsertOptions $options = null) : string
    {
        if ($options === null) {
            $options = new SqliteInsertOptions();
        }

        if (!$options instanceof SqliteInsertOptions) {
            throw new RuntimeException('Invalid insert options provided, expected MySQLInsertOptions got: ' . $options::class);
        }

        if ($options->conflictColumns) {
            return \sprintf(
                'INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s',
                $table->name(),
                \implode(',', \array_map(fn (string $column) : string => $this->platform->quoteIdentifier($column), $bulkData->columns()->all())),
                $bulkData->toSqlPlaceholders(),
                \implode(',', $options->conflictColumns),
                \count($options->updateColumns)
                    ? $this->updateSelectedColumns($options->updateColumns, $bulkData->columns())
                    : $this->updateAllColumns($bulkData->columns())
            );
        }

        if ($options->skipConflicts) {
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

    public function prepareUpdate(TableDefinition $table, BulkData $bulkData, ?UpdateOptions $updateOptions = null) : string
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
