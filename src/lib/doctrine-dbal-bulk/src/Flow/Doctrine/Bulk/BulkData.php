<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Types\Type;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use Flow\Types\Exception\InvalidTypeException;

use function array_chunk;
use function array_key_exists;
use function array_keys;
use function array_map;
use function array_merge;
use function array_values;
use function count;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function implode;
use function is_array;
use function is_string;
use function sprintf;
use function str_repeat;

final readonly class BulkData
{
    private Columns $columns;

    /**
     * @var array<int, array<string, mixed>>
     */
    private array $rows;

    /**
     * @param array<int, mixed> $rows
     * @param array<Type> $types
     */
    public function __construct(
        array $rows,
        private array $types = [],
        private SQLParametersStyle $parametersStyle = SQLParametersStyle::POSITIONAL,
    ) {
        if (0 === count($rows)) {
            throw new RuntimeException('Bulk data cannot be empty');
        }

        $rows = array_values($rows);
        $columns = is_array($rows[0]) ? array_keys($rows[0]) : [];
        $names = [];

        foreach ($columns as $column) {
            if (!is_string($column)) {
                throw new RuntimeException('Each row must be an array');
            }

            $names[] = $column;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($rows as $row) {
            if (is_array($row) && $columns === array_keys($row)) {
                continue;
            }

            try {
                type_list(type_map(type_string(), type_mixed()))->assert($rows);
            } catch (InvalidTypeException) {
                throw new RuntimeException('Each row must be an array');
            }

            throw new RuntimeException('Each row must be have the same keys in the same order');
        }

        /** @var array<int, array<string, mixed>> $rows */
        $this->columns = new Columns(...$names);
        $this->rows = $rows;
    }

    /**
     * @param int<1, max> $rows
     *
     * @return list<self>
     */
    public function chunk(int $rows): array
    {
        if (count($this->rows) <= $rows) {
            return [$this];
        }

        $chunks = [];

        foreach (array_chunk($this->rows, $rows) as $chunk) {
            $chunks[] = new self($chunk, $this->types, $this->parametersStyle);
        }

        return $chunks;
    }

    public function columns(): Columns
    {
        return $this->columns;
    }

    public function count(): int
    {
        return count($this->rows);
    }

    public function parametersStyle(): SQLParametersStyle
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
    public function rows(): array
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
    public function sqlRows(): array
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

    public function toSqlCastedPlaceholders(TableDefinition $table): string
    {
        return match ($this->parametersStyle) {
            SQLParametersStyle::NAMED => $this->toSqlNamedCastedPlaceholders($table),
            SQLParametersStyle::POSITIONAL => $this->toSqlPositionalCastedPlaceholders($table),
        };
    }

    public function toSqlNamedCastedPlaceholders(TableDefinition $table): string
    {
        return implode(',', array_map(
            /**
             * @param int $index
             * @param array<string, mixed> $row
             *
             * @return string
             */
            function (int $index, array $row) use ($table): string {
                $keys = [];

                /**
                 * @var mixed $_value
                 */
                foreach ($row as $columnName => $_value) {
                    if (array_key_exists($columnName, $this->types)) {
                        $type = $this->types[$columnName];
                    } else {
                        $type = $table->dbalColumn($columnName)->getType();
                    }

                    $keys[] =
                        'CAST(:'
                        . $columnName
                        . '_'
                        . $index
                        . ' as '
                        . $type->getSQLDeclaration([], $table->platform())
                        . ')';
                }

                return sprintf('(%s)', implode(',', $keys));
            },
            array_keys($this->rows),
            $this->rows,
        ));
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
    public function toSqlNamedParameters(TableDefinition $table): array
    {
        $rows = [];
        $platform = $table->platform();
        $types = [];

        foreach ($this->rows as $index => $row) {
            /**
             * @var mixed $entry
             */
            foreach ($row as $column => $entry) {
                $type =
                    $types[$column] ??= array_key_exists($column, $this->types)
                        ? $this->types[$column]
                        : $table->dbalColumn($column)->getType();
                $rows[$index][$column . '_' . $index] = $type->convertToDatabaseValue($entry, $platform);
            }
        }

        return array_merge(...$rows);
    }

    /**
     * @return string It returns a string for SQL bulk insert query, eg:
     *                (:id_0, :name_0, :title_0), (:id_1, :name_1, :title_1), (:id_2, :name_2, :title_2)
     */
    public function toSqlNamedPlaceholders(): string
    {
        return implode(',', array_map(static fn(array $row): string => sprintf('(:%s)', implode(
            ',:',
            array_keys($row),
        )), $this->sqlRows()));
    }

    /**
     * @return array<string, mixed>|list<mixed>
     */
    public function toSqlParameters(TableDefinition $table): array
    {
        return match ($this->parametersStyle) {
            SQLParametersStyle::NAMED => $this->toSqlNamedParameters($table),
            SQLParametersStyle::POSITIONAL => $this->toSqlPositionalParameters($table),
        };
    }

    public function toSqlPlaceholders(): string
    {
        return match ($this->parametersStyle) {
            SQLParametersStyle::NAMED => $this->toSqlNamedPlaceholders(),
            SQLParametersStyle::POSITIONAL => $this->toSqlPositionalPlaceholders(),
        };
    }

    public function toSqlPositionalCastedPlaceholders(TableDefinition $table): string
    {
        return implode(',', array_map(
            /**
             * @param array<string, mixed> $row
             *
             * @return string
             */
            function (array $row) use ($table): string {
                $keys = [];

                /**
                 * @var mixed $_value
                 */
                foreach ($row as $columnName => $_value) {
                    if (array_key_exists($columnName, $this->types)) {
                        $type = $this->types[$columnName];
                    } else {
                        $dbColumn = $table->dbalColumn($columnName);
                        $type = $dbColumn->getType();
                    }

                    $keys[] = 'CAST(? as ' . $type->getSQLDeclaration([], $table->platform()) . ')';
                }

                return sprintf('(%s)', implode(',', $keys));
            },
            $this->rows,
        ));
    }

    /**
     * Example:.
     *
     * [1, 'some name', 2, 'other name']
     *
     * @return list<mixed>
     */
    public function toSqlPositionalParameters(TableDefinition $table): array
    {
        $parameters = [];
        $platform = $table->platform();
        $types = [];

        foreach ($this->rows as $row) {
            /**
             * @var mixed $entry
             */
            foreach ($row as $column => $entry) {
                $type =
                    $types[$column] ??= array_key_exists($column, $this->types)
                        ? $this->types[$column]
                        : $table->dbalColumn($column)->getType();
                $parameters[] = $type->convertToDatabaseValue($entry, $platform);
            }
        }

        return $parameters;
    }

    /**
     * @return string It returns a string for SQL bulk insert query with positional parameters, eg:
     *                (?,?,?), (?,?,?), (?,?,?)
     */
    public function toSqlPositionalPlaceholders(): string
    {
        $columnCount = count($this->columns->all());
        $rowCount = $this->count();

        $rowPlaceholder = '(' . str_repeat('?,', max(0, $columnCount - 1)) . '?)';

        return str_repeat($rowPlaceholder . ',', max(0, $rowCount - 1)) . $rowPlaceholder;
    }

    /**
     * @return array<Type>
     */
    public function types(): array
    {
        return $this->types;
    }
}
