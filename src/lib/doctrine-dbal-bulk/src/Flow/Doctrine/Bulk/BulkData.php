<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Types\Types;
use Flow\Doctrine\Bulk\Exception\RuntimeException;

final class BulkData
{
    private Columns $columns;

    /**
     * @var array<int, array<string, mixed>>
     */
    private array $rows;

    /**
     * @psalm-suppress DocblockTypeContradiction
     *
     * @param array<int, array<string, mixed>> $rows
     */
    public function __construct(array $rows)
    {
        if (empty($rows)) {
            throw new RuntimeException('Bulk data cannot be empty');
        }

        $firstRow = \reset($rows);

        if (!\is_array($firstRow)) {
            throw new RuntimeException('Each row must be an array');
        }

        $columns = \array_keys($firstRow);

        foreach ($rows as $row) {
            if (!\is_array($row)) {
                throw new RuntimeException('Each row must be an array');
            }

            if ($columns !== \array_keys($row)) {
                throw new RuntimeException('Each row must be have the same keys in the same order');
            }
        }

        $this->columns = new Columns(...$columns);
        $this->rows = \array_values($rows);
    }

    public function columns() : Columns
    {
        return $this->columns;
    }

    public function count() : int
    {
        return \count($this->rows);
    }

    /**
     * Example:.
     *
     * [
     *   ['id' => 1, 'name' => 'some name'],
     *   ['id' => 2, 'name' => 'other name'],
     * ]
     *
     * @return array<int, array<string, mixed>>
     */
    public function rows() : array
    {
        return $this->rows;
    }

    /**
     * Example:.
     *
     * [
     *   ['id_0' => 1, 'name_0' => 'some name'],
     *   ['id_1' => 2, 'name_1' => 'other name'],
     * ]
     *
     * @return array<int, array<string, mixed>>
     */
    public function sqlRows() : array
    {
        $rows = [];

        foreach ($this->rows as $index => $row) {
            /**
             * @var mixed $entry
             */
            foreach ($row as $column => $entry) {
                $rows[$index][$column . '_' . $index] = $entry;
            }
        }

        return $rows;
    }

    /**
     * Example:.
     *
     * [
     *   'id_0' => 1, 'name_0' => 'some name',
     *   'id_1' => 2, 'name_1' => 'other name',
     * ]
     *
     * @return array<string, mixed>
     *
     * @psalm-suppress DeprecatedMethod
     */
    public function toSqlParameters(TableDefinition $table) : array
    {
        $rows = [];

        foreach ($this->rows as $index => $row) {
            /**
             * @var mixed $entry
             */
            foreach ($row as $column => $entry) {
                if (\is_string($entry) && ($table->dbalColumn($column)->getType()->getName() === Types::JSON || $table->dbalColumn($column)->getType()->getName() === 'json_array')) {
                    $rows[$index][$column . '_' . $index] = \json_decode($entry, true, 512, JSON_THROW_ON_ERROR);
                } else {
                    $rows[$index][$column . '_' . $index] = $entry;
                }
            }
        }

        return \array_merge(...$rows);
    }

    /**
     * @return string It returns a string for SQL bulk insert query, eg:
     *                (:id_0, :name_0, :title_0), (:id_1, :name_1, :title_1), (:id_2, :name_2, :title_2)
     */
    public function toSqlPlaceholders() : string
    {
        return \implode(
            ',',
            \array_map(
                fn (array $row) : string => \sprintf(
                    '(:%s)',
                    \implode(',:', \array_keys($row))
                ),
                $this->sqlRows()
            )
        );
    }
}
