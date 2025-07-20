<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Types\{Type};
use Flow\Doctrine\Bulk\Exception\RuntimeException;

final readonly class BulkData
{
    private Columns $columns;

    /**
     * @var array<int, array<string, mixed>>
     */
    private array $rows;

    /**
     * @param array<int, array<string, mixed>> $rows
     * @param array<Type> $types
     */
    public function __construct(array $rows, private array $types = [], private SQLParametersStyle $parametersStyle = SQLParametersStyle::POSITIONAL)
    {
        if (0 === \count($rows)) {
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

    public function parametersStyle() : SQLParametersStyle
    {
        return $this->parametersStyle;
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

    public function toSqlCastedPlaceholders(TableDefinition $table) : string
    {
        return match ($this->parametersStyle) {
            SQLParametersStyle::NAMED => $this->toSqlNamedCastedPlaceholders($table),
            SQLParametersStyle::POSITIONAL => $this->toSqlPositionalCastedPlaceholders($table),
        };
    }

    public function toSqlNamedCastedPlaceholders(TableDefinition $table) : string
    {
        return \implode(
            ',',
            \array_map(
                /**
                 * @param int $index
                 * @param array<string, mixed> $row
                 *
                 * @return string
                 */
                function (int $index, array $row) use ($table) : string {
                    $keys = [];

                    /**
                     * @var mixed $value
                     */
                    foreach ($row as $columnName => $value) {
                        if (\array_key_exists($columnName, $this->types)) {
                            $type = $this->types[$columnName];
                        } else {
                            $type = $table->dbalColumn($columnName)->getType();
                        }

                        $keys[] = 'CAST(:' . $columnName . '_' . $index . ' as ' . $type->getSQLDeclaration([], $table->platform()) . ')';
                    }

                    return \sprintf(
                        '(%s)',
                        \implode(',', $keys)
                    );
                },
                \array_keys($this->rows),
                $this->rows,
            )
        );
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
     */
    public function toSqlNamedParameters(TableDefinition $table) : array
    {
        $rows = [];

        foreach ($this->rows as $index => $row) {
            /**
             * @var mixed $entry
             */
            foreach ($row as $column => $entry) {
                if (\array_key_exists($column, $this->types)) {
                    $value = $this->types[$column]->convertToDatabaseValue($entry, $table->platform());
                } else {
                    $value = $table->dbalColumn($column)->getType()->convertToDatabaseValue($entry, $table->platform());
                }

                $rows[$index][$column . '_' . $index] = $value;
            }
        }

        return \array_merge(...$rows);
    }

    /**
     * @return string It returns a string for SQL bulk insert query, eg:
     *                (:id_0, :name_0, :title_0), (:id_1, :name_1, :title_1), (:id_2, :name_2, :title_2)
     */
    public function toSqlNamedPlaceholders() : string
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

    /**
     * @return array<int<0, max>|string, mixed>
     */
    public function toSqlParameters(TableDefinition $table) : array
    {
        return match ($this->parametersStyle) {
            SQLParametersStyle::NAMED => $this->toSqlNamedParameters($table),
            SQLParametersStyle::POSITIONAL => $this->toSqlPositionalParameters($table),
        };
    }

    public function toSqlPlaceholders() : string
    {
        return match ($this->parametersStyle) {
            SQLParametersStyle::NAMED => $this->toSqlNamedPlaceholders(),
            SQLParametersStyle::POSITIONAL => $this->toSqlPositionalPlaceholders(),
        };
    }

    public function toSqlPositionalCastedPlaceholders(TableDefinition $table) : string
    {
        return \implode(
            ',',
            \array_map(
                /**
                 * @param array<string, mixed> $row
                 *
                 * @return string
                 */
                function (array $row) use ($table) : string {
                    $keys = [];

                    /**
                     * @var mixed $value
                     */
                    foreach ($row as $columnName => $value) {
                        if (\array_key_exists($columnName, $this->types)) {
                            $type = $this->types[$columnName];
                        } else {
                            $dbColumn = $table->dbalColumn($columnName);
                            $type = $dbColumn->getType();
                        }

                        $keys[] = 'CAST(? as ' . $type->getSQLDeclaration([], $table->platform()) . ')';
                    }

                    return \sprintf(
                        '(%s)',
                        \implode(',', $keys)
                    );
                },
                $this->rows
            )
        );
    }

    /**
     * Example:.
     *
     * [1, 'some name', 2, 'other name']
     *
     * @return array<int<0, max>, mixed>
     */
    public function toSqlPositionalParameters(TableDefinition $table) : array
    {
        $parameters = [];

        foreach ($this->rows as $row) {
            /**
             * @var mixed $entry
             */
            foreach ($row as $column => $entry) {
                if (\array_key_exists($column, $this->types)) {
                    $value = $this->types[$column]->convertToDatabaseValue($entry, $table->platform());
                } else {
                    $value = $table->dbalColumn($column)->getType()->convertToDatabaseValue($entry, $table->platform());
                }

                $parameters[] = $value;
            }
        }

        return $parameters;
    }

    /**
     * @return string It returns a string for SQL bulk insert query with positional parameters, eg:
     *                (?,?,?), (?,?,?), (?,?,?)
     */
    public function toSqlPositionalPlaceholders() : string
    {
        $columnCount = \count($this->columns->all());
        $rowCount = $this->count();

        $rowPlaceholder = '(' . \str_repeat('?,', $columnCount - 1) . '?)';

        return \str_repeat($rowPlaceholder . ',', $rowCount - 1) . $rowPlaceholder;
    }

    /**
     * @return array<Type>
     */
    public function types() : array
    {
        return $this->types;
    }
}
